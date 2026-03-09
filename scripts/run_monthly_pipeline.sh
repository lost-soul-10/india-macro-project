#!/bin/bash

cd ~/Downloads/india-macro-project || exit 1

source .venv/bin/activate

python scripts/fetch_wpi_mospi.py
python scripts/build_wpi_features.py