#!/bin/bash

# Install workspace in editable mode
pip install --user -e /workspace[dev]

# Copy config and credentials sample files
cp "./scripts/config.sample.py" "./scripts/config.py"
cp "./scripts/credentials.sample.py" "./scripts/credentials.py"