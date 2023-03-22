# export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
# set | grep PROTOCOL


# pip install web3==5.31.4
# pip install web3==6.0.0b6

# multicall installs web3==5.31.4 which doesn't work on python 3.10
# install web3==6.0.0 over the top - multicall still appears to work
# web3==6.0.0 has protobuf > 4.2 which is OK for dagster now
pip install multicall==0.7.1
pip install web3==6.0.0
# pip install protobuf==3.20.3