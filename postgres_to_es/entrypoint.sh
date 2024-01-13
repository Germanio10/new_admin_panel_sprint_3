#!/bin/bash

set -x

sleep 30

exec python3 main.py "$@"
