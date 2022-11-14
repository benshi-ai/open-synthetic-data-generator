#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "apply_migrations.sh <yaml_config_filename>"
  exit
fi

alembic -x config_filename=$1 upgrade head
