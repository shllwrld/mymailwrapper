# mymailwrapper
my.mail.ru people search wrapper

```
usage: mymailwrapper.py [-h] [--debug] [--quite] [--timeout TIMEOUT]
                        {search,show,auth,update_geo} ...

optional arguments:
  -h, --help            show this help message and exit
  --debug               increase verbosity level
  --quite               decrease verbosity level
  --timeout TIMEOUT     set custom timeout between requests (default 5s)

action:
  {search,show,auth,update_geo}
    search              search for accounts (search -h for more help) and dump
                        result into csv file
    show                show constants (show -h for more help)
    auth                authenticate and save cookies to session file
    update_geo          update geo info from my.mail.ru database
```
