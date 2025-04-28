#!/bin/bash

# Set environment variable for Flask app
export FLASK_APP=lwfm/midware/impl/LwfmEventSvc

# Start Flask server in the background
source ./.venv/bin/activate 
flask run -p 3000 &
FLASK_PID=$!
echo Flask PID = $FLASK_PID

# Start tailing the repo file in the background if it exists
#REPO_FILE="$HOME/.lwfm/lwfm.repo"
#if [ -f "$REPO_FILE" ]; then
#    xterm -e "tail -n 20 -F \"$REPO_FILE\"" &
#    TAIL_PID=$!
#fi
#echo Tail PID = $TAIL_PID

# Function to clean up background processes
cleanup() {
    if [ -n "$FLASK_PID" ]; then
        echo "Stopping Flask server..."
        kill $FLASK_PID 2>/dev/null
    fi
#    if [ -n "$TAIL_PID" ]; then
#        echo "Stopping log tail..."
#        kill $TAIL_PID 2>/dev/null
#    fi
    exit 0
}

# Trap INT and TERM signals to clean up
trap cleanup INT TERM

# Keep alive until interrupted
while true; do
    sleep 1
done










