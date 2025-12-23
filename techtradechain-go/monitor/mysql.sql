CREATE DATABASE grafana DEFAULT CHARACTER SET utf8mb4;
CREATE USER 'techtradechain'@'%' IDENTIFIED BY 'techtradechain';
GRANT all privileges ON grafana.* TO 'techtradechain'@'%';
FLUSH PRIVILEGES;