# export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
# set | grep PROTOCOL

# install web3 reverts protobuf to 3.20.1 which breaks google libs.  Revert after to fix.
pip install web3==6.0.0b6
pip install protobuf==3.20.3