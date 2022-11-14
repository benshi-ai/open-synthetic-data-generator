#!/bin/bash

export SYNTHETIC_DATA_GENERATOR_PATH="$HOME/synthetic-data-generator/src/"
source $HOME/synthetic-data-generator/service/env.sh
cd $HOME/synthetic-data-generator/src/synthetic/
exec python synthetic_data.py ../../data/config/$1.yml
