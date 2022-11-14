On an ubuntu server:
1. Copy synthetic-data-*.service to /etc/systemd/system/
2. Set service to run on boot with `sudo systemctl enable synthetic-data-<org_proj>.service`
3. start service with `sudo systemctl start synthetic-data-<org_proj>.service`
4. Tail logs that are in synthetic-data/src/synthetic/logs
5. Or look in journalctl: `sudo journalctl -u synthetic-data-<org_proj>.service`
