#!/bin/bash

REMOTE_HOSTS="wd312@caelum-103"

# Download logs
rm -rf ~/Code/scheduling/dodoor/deploy/resources/log/*
scp -r $REMOTE_HOSTS:~/Code/scheduling/dodoor/deploy/resources/log/* ~/Code/scheduling/dodoor/deploy/resources/log/.

# Plot
python3 deploy/python/scripts/plot.py
