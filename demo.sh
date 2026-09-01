#!/bin/bash

source .venv/bin/activate

python producer.py \
    --eventos 0 \
    --tps 1 \
    --anomalias 20
