#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "make_migrations.sh <yaml_config_filename> <migration_message>"
  exit
fi

alembic -x config_filename=$1 revision --autogenerate -m "$2"
