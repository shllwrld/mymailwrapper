import requests
import json
import pickle
import os
import logging
import argparse
from time import sleep
from bs4 import BeautifulSoup

# logging settings
LOG_LEVELS = {
    'CRITICAL': logging.CRITICAL,
    'ERROR': logging.ERROR,
    'WARNING': logging.WARNING,
    'INFO': logging.INFO,
    'DEBUG': logging.DEBUG
}
LOG_LEVEL = LOG_LEVELS.get("INFO")
logger = logging.getLogger()
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_logger = logging.StreamHandler()
console_logger.setFormatter(formatter)
logger.addHandler(console_logger)
logger.setLevel(LOG_LEVEL)


# script globals
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36.",
    "X-Requested-With": "XMLHttpRequest"
}
SESSION_FILE = "session"
GEO_FILE = "geo_data.json"
CSV_COLUMNS = [
    "ID",
    "FirstName",
    "LastName",
    "Email",
    "Age",
    "Sex",
    "CountryName",
    "RegionName",
    "CityName",
    "AvatarURL",
    "LastVisit",
    "AuID",
]
LOG_COLUMNS = [
    "ID",
    "FirstName",
    "LastName",
    "Email",
    "Age",
    "CountryName",
    "RegionName",
    "CityName",
]
COUNTRIES_SHORT = [
    "Россия",
    "Украина",
    "Беларусь",
    "Казахстан",
    "Грузия",
    "Армения",
    "Азербайджан",
    "Таджикистан",
    "Узбекистан",
]
PAGINATION = 10
LIMIT = 500


class MyMailSearch:
    def __init__(self, timeout, *args, **kwargs):
        self.session = requests.Session()
        self._geo_file = GEO_FILE
        self.result_prefix = "result"
        self.result_postfix = "my.mail.ru.csv"
        self.result_file = None
        self.pagination = PAGINATION
        self.limit = LIMIT
        self.timeout = timeout
        self.geo_data = None

    def init_geo(self):
        try:
            with open(self._geo_file, "r") as f:
                self.geo_data = json.load(f)
        except FileNotFoundError:
            logger.error("Geo file not found. Try to run script with update_geo command")

    def authenticate(self, username, password, domain):
        page = 'https://my.mail.ru/'
        fail_page = 'https://my.mail.ru/cgi-bin/login?fail=1'
        fail_url = 'https://my.mail.ru/?fail=1'
        payload = {
            "page": page,
            "FailPage": fail_page,
            "Domain": domain,
            "Login": username,
            "Password": password,
        }
        d = self.session.post("https://auth.mail.ru/cgi-bin/auth", data=payload)
        if d.url == fail_url:
            logger.error("Login data incorrect")
            return False
        else:
            with open(SESSION_FILE, "wb") as f:
                pickle.dump(self.session.cookies, f)
            logger.debug("Auth success. Session saved")
            return True

    @staticmethod
    def get_search_url(name, offset, *args, **kwargs):
        base = "https://my.mail.ru/cgi-bin/my/ajax?&ajax_call=1&func_name=search.get"
        arg_name = f"&arg_name={name}"
        arg_offset = f"&arg_offset={offset}"
        optional = ""
        if "country" in kwargs.keys():
            optional += f"&arg_countryid={kwargs['country']}"
        if "region" in kwargs.keys():
            optional += f"&arg_region={kwargs['region']}"
        if "city" in kwargs.keys():
            optional += f"&arg_cityid={kwargs['city']}"
        return base + arg_name + arg_offset + optional

    def get_search_data(self, search_name, offset, *args, **kwargs):
        logger.info(f"offset {offset}")
        url = self.get_search_url(search_name, offset, *args, **kwargs)
        while True:
            r = self.session.get(url)
            try:
                data = json.loads(r.content.decode("utf-8"))[2]
                break
            except json.decoder.JSONDecodeError as e:
                if "https://help.mail.ru/my/access/unban/" in r.content.decode("utf-8"):
                    logger.warning("Flood wait..")
                    sleep(360)
                else:
                    logger.error(str(e))
                    logger.error("Unknown response. Retrying...")
                    sleep(self.timeout)
        return data

    def update_geo_data(self):
        def get_geo_data(url):
            while True:
                try:
                    r = self.session.get(url)
                    data = json.loads(r.content.decode("utf-8"))[2]["result"]
                    break
                except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError) as e:
                    sleep(self.timeout)
                    logger.error(str(e))
                    logger.error("Error. Retrying...")
            return data

        database = []
        update_url = "https://my.mail.ru/cgi-bin/my/ajax?ajax_call=1&arg_offset=0&func_name=geo."
        countries = get_geo_data(update_url + "countries")
        for i_c, country in enumerate(countries):
            logger.info(f'{country["country_id"]} - {country["name"]}')
            regions = get_geo_data(update_url + "regions" + f"&arg_country={country['country_id']}")
            for i_r, region in enumerate(regions):
                cities = get_geo_data(update_url + "cities" + f"&arg_region={region['region_id']}")
                regions[i_r].update({"cities": cities})
            countries[i_c].update({"regions": regions})
            database.append(country)
        with open(self._geo_file, "w") as f:
            json.dump(database, f)

    def show_countries(self, all_countries=False):
        self.init_geo()
        if all_countries:
            self.columns_print(self.geo_data, "country_id")
        else:
            for country in self.geo_data:
                if country['name'] in COUNTRIES_SHORT:
                    print(f"{country['country_id']} {country['name']}")

    def show_regions(self, country_id):
        self.init_geo()
        for country in self.geo_data:
            if str(country_id) == country["country_id"]:
                if len(country["regions"]) > 0:
                    self.columns_print(country["regions"], "region_id")
                else:
                    logger.error("No regions found for this country_id")

    def show_cities(self, country_id, region_id):
        self.init_geo()
        for country in self.geo_data:
            if str(country_id) == country["country_id"]:
                if len(country["regions"]) > 0:
                    for region in country["regions"]:
                        if str(region_id) == region["region_id"]:
                            if len(region["cities"]) > 0:
                                self.columns_print(region["cities"], "city_id")
                            else:
                                logger.error("No cities found for this country_id")
                else:
                    logger.error("No regions found for this country_id")

    @staticmethod
    def columns_print(data: list, param_id: str):
        max_id_len = 0
        max_name_len = 0
        for i in data:
            if len(i[param_id]) > max_id_len:
                max_id_len = len(i[param_id])
            if len(i["name"]) > max_name_len:
                max_name_len = len(i["name"])
        max_name_len += 1
        while len(data) % 3 != 0:
            data.append({param_id: " ", "name": " "})
        for a, b, c in zip(data[::3], data[1::3], data[2::3]):
            print(f'{a[param_id]:<{max_id_len}} {a["name"]:<{max_name_len}}'
                  f'{b[param_id]:<{max_id_len}} {b["name"]:<{max_name_len}}'
                  f'{c[param_id]:<{max_id_len}} {c["name"]:<{max_name_len}}')

    def write_line(self, user_data):
        for user in user_data:
            user_data = []
            for column in CSV_COLUMNS:
                try:
                    user_data.append(str(user[column]))
                except KeyError:
                    user_data.append("")
            with open(self.result_file, "a") as f:
                f.write(";".join(user_data) + '\r\n')

    def auth_interactive(self):
        logger.info("Interactive mode")
        is_auth = False
        for i in range(3):
            # sleep to avoid possible cross of logger output and user input / bad way
            sleep(1)
            domain = input("Enter domain (mail.ru/list.ru/...): ")
            username = input("Enter username: ")
            password = input("Enter password: ")
            is_auth = self.authenticate(username, password, domain)
            if is_auth:
                logger.info("Auth success. Cookies saved to session file")
                break
        if not is_auth:
            logger.error("3 incorrect attempts. Exit")
            exit(0)

    def check_session(self):
        if os.path.isfile(SESSION_FILE):
            logger.info("Previous session found. Loading...")
            with open(SESSION_FILE, 'rb') as f:
                self.session.cookies.update(pickle.load(f))
            check = self.session.get("https://auth.mail.ru/cgi-bin/auth")
            if check.url == "https://e.mail.ru/messages/inbox/":
                data = self.get_search_data("test query", 0)
                if data == "https://auth.mail.ru/sdc?fail=https%3A%2F%2F" \
                           "my.mail.ru%2Fcgi-bin%2Flogin%3Fnoredir%3D1&from=":
                    logger.error("Auth check passed, but api deny your request. Authenticate...")
                    self.auth_interactive()
                logger.info("Auth check passed")
            elif check.url == 'https://account.mail.ru/login?&fail=1':
                logger.error("Auth check failed. Authenticate...")
                self.auth_interactive()
            else:
                logger.error("Auth check returned unknown response")
        else:
            logger.debug("Previous session not found. Authenticate...")
            self.auth_interactive()

    @staticmethod
    def log_line(users):
        for user in users:
            user_data = []
            for column in LOG_COLUMNS:
                try:
                    user_data.append(str(user[column]))
                except KeyError:
                    user_data.append("")
            logger.info(f'{" ".join(user_data)}')

    def search(self, search_name, *args, **kwargs):
        self.init_geo()
        data = self.get_search_data(search_name, 0, *args, **kwargs)
        data_len = data.get("total")
        logger.info(f"Found {data_len} profiles")
        self.result_file = f"{self.result_prefix}-{search_name}-{self.result_postfix}"
        if int(data_len) < 500:
            self.limit = int(data_len)
        for offset in range(0, self.limit, self.pagination):
            if offset == 0:
                with open(self.result_file, "w") as f:
                    f.write(";".join(CSV_COLUMNS) + '\r\n')
            else:
                data = self.get_search_data(search_name, offset)
            self.write_line(data["users"])
            self.log_line(data["users"])
            sleep(self.timeout)
        logger.info(f"search \"{search_name}\" done")

    def search_simple(self, search_name):
        def proceed_list(data: list):
            for a in a_list:
                link = a['href']
                link_data = link.split('/')
                domain = link_data[1]
                if domain in ['mail', 'inbox', 'list', 'bk']:
                    domain = domain + ".ru"
                username = link_data[2]
                bio_info = a.find("span", {"class": "list-item__title"}).get_text()
                geo_info = a.find("span", {"class": "list-item__info"})
                if geo_info:
                    geo_info = geo_info.get_text()
                else:
                    geo_info = ""
                logger.info(f'{username}@{domain} {bio_info} {geo_info}')
                with open(self.result_file, "a", encoding="utf-8") as f:
                    f.write(f'{username}@{domain};{bio_info};{geo_info}\n')

        host = 'https://m.my.mail.ru/my'
        url = 'search_people?st=search&common='
        self.result_file = f"{self.result_prefix}-{search_name}-{self.result_postfix}"
        with open(self.result_file, "w") as f:
            f.write(f'email;bio_info;geo_info\n')

        for i in range(1, 50):
            logger.info(f'processing page {i}...')
            r = self.session.get(f'{host}/{url}{search_name}&p={i}')
            soup = BeautifulSoup(r.content, 'html.parser')
            a_list = soup.find_all('a', {'class': 'list-item__link'})
            if len(a_list) == 0:
                break
            elif len(a_list) < 10:
                proceed_list(a_list)
                break
            else:
                proceed_list(a_list)
            sleep(self.timeout)
        logger.info(f'done')


def console_run():
    parent_parser = argparse.ArgumentParser(add_help=False)
    parent_parser.add_argument("--debug", help="increase verbosity level", action="store_true")
    parent_parser.add_argument("--quite", help="decrease verbosity level", action="store_true")
    parent_parser.add_argument("--timeout", help="set custom timeout between requests (default 5s)",
                               default=5, type=int)

    main_parser = argparse.ArgumentParser(description='my.mail.ru people search wrapper',
                                          parents=[parent_parser])
    cmd_subparsers = main_parser.add_subparsers(title="action", dest="cmd")
    search_parser = cmd_subparsers.add_parser("search", help="search for accounts (search -h for more help) "
                                                             "and dump result into csv file",
                                              parents=[parent_parser])
    show_parser = cmd_subparsers.add_parser("show", help="show constants (show -h for more help)",
                                            parents=[parent_parser])
    auth_parser = cmd_subparsers.add_parser("auth", help="authenticate and save cookies to session file",
                                            parents=[parent_parser])
    auth_parser.add_argument("-username")
    auth_parser.add_argument("-password")
    auth_parser.add_argument("-domain")
    auth_parser.add_argument("--i", dest="interactive", help="interactive_authenticate",
                             action="store_true", default=False)

    update_parser = cmd_subparsers.add_parser("update_geo", help="update geo info from my.mail.ru database")

    search_parser.add_argument('search_name', help='name for search')
    search_parser.add_argument('-country', help='set country for search', type=int)
    search_parser.add_argument('-region', help='set region for search', type=int)
    search_parser.add_argument('-city', help='set city for search', type=int)
    # search_parser.add_argument('-age', help='set age for search', type=int) to_do
    search_parser.add_argument('--simple', help='search via m.my.mail.ru engine', action='store_true', default=False)

    geo_subparsers = show_parser.add_subparsers(title="object", dest="geo")
    country_parser = geo_subparsers.add_parser("countries")
    region_parser = geo_subparsers.add_parser("regions")
    city_parser = geo_subparsers.add_parser("cities")
    country_parser.add_argument('--all', help='get all available countries', default=False, action='store_true')
    region_parser.add_argument('country_id', type=int)
    city_parser.add_argument('country_id', type=int)
    city_parser.add_argument('region_id', type=int)

    args = main_parser.parse_args()
    cmd = args.cmd

    if args.debug:
        logger.setLevel(LOG_LEVELS["DEBUG"])
    if args.quite:
        logger.setLevel(LOG_LEVELS["WARNING"])

    if cmd:
        s = MyMailSearch(timeout=args.timeout)
        s.session.headers.update(HEADERS)

        if cmd == "search":
            optional = ["country", "region", "city"]
            kwargs = {}
            for key, value in vars(args).items():
                if key in optional and value:
                    kwargs.update({key: value})
            search_name = args.search_name
            s.check_session()
            if args.simple:
                s.search_simple(search_name)
            else:
                s.search(search_name, **kwargs)
        elif cmd == "show":
            if args.geo == "countries":
                if args.all:
                    s.show_countries(all_countries=True)
                else:
                    s.show_countries(all_countries=False)
            elif args.geo == "regions":
                s.show_regions(args.country_id)
            elif args.geo == "cities":
                s.show_cities(args.country_id, args.region_id)
        elif cmd == "update_geo":
            s.check_session()
            s.update_geo_data()
        elif cmd == "auth":
            if args.interactive:
                s.auth_interactive()
            else:
                if args.username is None or args.password is None or args.domain is None:
                    auth_parser.error("username, password and domain required")
                else:
                    s.authenticate(args.username, args.password, args.domain)
    else:
        main_parser.print_help()


if __name__ == "__main__":
    console_run()
