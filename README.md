cd techtradechain-cryptogen
make

cd ..

cd techtradechain-go/tools
ln -s ../../techtradechain-cryptogen/ .

cd ../scripts
./prepare.sh 4 1
./build_release.sh
./cluster_quick_start.sh normal
