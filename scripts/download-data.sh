#!/bin/bash

set -e

mkdir -p data
cd data
wget http://www.iro.umontreal.ca/~lisa/datasets/profiledata_06-May-2005.tar.gz
tar xvf profiledata_06-May-2005.tar.gz
mv profiledata_06-May-2005/* .
rmdir profiledata_06-May-2005
