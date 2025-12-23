技术贸易链 TechTradeChain

下载 https://go.dev/dl/go1.24.11.linux-amd64.tar.gz，配置go环境

安装gcc等开发包   sudo apt install build-essential

本项目是根据长安链开源代码修改

techtradechain-cryptogen 从 chainmaker-cryptogen v2.3.5 修改

techtradechain-go 从 chainmaker-go v2.3.7 修改



cd techtradechain-cryptogen

make

cd ..

cd techtradechain-go/tools

ln -s ../../techtradechain-cryptogen/ .

cd ../scripts

./prepare.sh 4 1

./build_release.sh

./cluster_quick_start.sh normal
