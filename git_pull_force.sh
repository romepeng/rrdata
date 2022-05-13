#!/bin/bash

cd ~/rrdata
conda activate rrsdk

git fetch --all
git reset --hard origin/master 
git pull

pip install .