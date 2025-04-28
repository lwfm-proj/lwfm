#!/bin/bash

python ./lwfm/util/startMidware.py & 
FLASK_PID=$!
echo Flask PID = $FLASK_PID

# Function to clean up background processes
cleanup() {
    if [ -n "$FLASK_PID" ]; then
        echo "Stopping Flask server..."
        kill $FLASK_PID 2>/dev/null
    fi
    exit 0
}

# Trap INT and TERM signals to clean up
trap cleanup INT TERM

# Keep alive until interrupted
while true; do
    sleep 1
done
