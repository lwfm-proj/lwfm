#!/bin/bash

# create ~/.lwfm/logs if it doesn't exist
mkdir -p ~/.lwfm
mkdir -p ~/.lwfm/logs


python ./lwfm/midware/_impl/SvcLauncher.py > ~/.lwfm/logs/midware.log 2>&1 &
FLASK_PID=$!
echo Flask PID = $FLASK_PID


# Function to clean up background processes
cleanup() {
    echo " * Caught exit signal - propagating... "
    if [ -n "$FLASK_PID" ]; then
        kill -SIGINT -- -$FLASK_PID 2>/dev/null
        sleep 5
        if ps -p $FLASK_PID > /dev/null; then
            kill -9 -- -$FLASK_PID 2>/dev/null
        fi
    fi
    mv ~/.lwfm/logs/midware.log ~/.lwfm/logs/midware-$FLASK_PID.log
    echo " * DONE"
    exit 0
}

# Trap INT and TERM signals to clean up
trap cleanup INT TERM

tail -f ~/.lwfm/logs/midware.log 

