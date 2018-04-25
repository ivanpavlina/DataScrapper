# DataScrapper

Mikrotik setup:

Enable Accounting:
ip accounting set enabled=yes account-local-traffic=yes

Enable API access and add read user:
ip service set api disabled=no
user add name=api_read group=read password=12345


Optional:

If you don't want to collect local network traffic:
ip accounting set account-local-traffic=no
