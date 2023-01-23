# this resolves an install error with protobuf
export PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python
pip3 install --upgrade pip

# git is required for installing pip packages from github repos
# gcc is required to install pyethash (ape dependency)
apt update
apt-get -y install git gcc

