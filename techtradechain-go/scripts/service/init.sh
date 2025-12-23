sudo cp techtradechain.service  /etc/systemd/system
sudo systemctl daemon-reload
sudo systemctl start techtradechain
sudo systemctl enable techtradechain
sudo systemctl status techtradechain
