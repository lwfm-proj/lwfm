#!/bin/bash
# Start the lwfm middleware - a REST service, a remote site poller, a DB maybe. 
# See the SvcLauncher for details.

# Resolve script directory for robust relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ~/.lwfm will contain config files and logs
# create if it doesn't exist
mkdir -p ~/.lwfm
mkdir -p ~/.lwfm/logs

touch ~/.lwfm/logs/midware.log ~/.lwfm/logs/launcher.log

export PYTHONUNBUFFERED=1

# Function to check if Flask server is running
check_flask_running() {
    # Check if middleware is running on port 3000
    if curl -s http://127.0.0.1:3000/isRunning >/dev/null 2>&1; then
        return 0  # running
    else
        return 1  # not running
    fi
}

# Function to launch GUI
launch_gui() {
    echo "Launching GUI..."   
    python ./src/lwfm/midware/_impl/gui/run_gui.py # >> ~/.lwfm/logs/gui.log 2>&1 &
}

# function to clean up background processes
cleanup() {
    echo " * Caught exit signal - propagating... "
    if [ -n "$FLASK_PID" ]; then
    # terminate entire process group
    kill -SIGINT -- -$FLASK_PID 2>/dev/null
        sleep 5
        if ps -p $FLASK_PID > /dev/null; then
        kill -9 -- -$FLASK_PID 2>/dev/null
        fi
    fi
    # belt-and-suspenders: if middleware pid file exists, kill that session too
    if [ -f ~/.lwfm/logs/midware.PID ]; then
        MPID=$(cat ~/.lwfm/logs/midware.PID 2>/dev/null)
        if [ -n "$MPID" ]; then
            kill -TERM -- -$MPID 2>/dev/null
            sleep 2
            if ps -p $MPID > /dev/null; then
                kill -KILL -- -$MPID 2>/dev/null
            fi
        fi
    fi

    # rotate the log files for safe keeping using a timestamp suffix (not PIDs)
    ts=$(date +"%Y%m%d-%H%M%S")
    mv ~/.lwfm/logs/midware.log ~/.lwfm/logs/midware-$ts.log
    mv ~/.lwfm/logs/launcher.log ~/.lwfm/logs/launcher-$ts.log
    if [ -f ~/.lwfm/logs/gui.log ]; then
        mv ~/.lwfm/logs/gui.log ~/.lwfm/logs/gui-$ts.log
    fi
    echo " * DONE"
    exit 0
}


# Handle command line arguments first
case "${1:-}" in
    "help"|"--help"|"-h")
        echo "Usage: $0 [command]"
        echo "Commands:"
        echo "  (no args)  Start Flask server and GUI"
        echo "  gui        Launch GUI"
        echo "  status     Show status of Flask server"
        echo "  help       Show this help message"
        exit 0
        ;;
    "status")
        echo "=== LWFM Status ==="
        if check_flask_running; then
            echo "✓ Flask server: RUNNING (port 3000)"
        else
            echo "✗ Flask server: NOT RUNNING"
        fi
        exit 0
        ;;
    "gui")
        launch_gui
        exit 0
        ;;
    "stop")
        echo "Stopping Flask server..."
        cleanup
        # cleanup exits the script
        ;;
esac


# Check if Flask server is already running
if check_flask_running; then
    echo "Flask server is already running on port 3000"
    FLASK_PID=$(pgrep -f "SvcLauncher.py" | head -1)
    if [ -n "$FLASK_PID" ]; then
        echo "$FLASK_PID" > ~/.lwfm/logs/midware.PID
    fi
else
    echo "Starting Flask server..."
    # start with a clean log only when starting a new server session
    : > ~/.lwfm/logs/midware.log
    : > ~/.lwfm/logs/launcher.log
    # launch the middleware in the background and route stdout and stderr to a log file
    python "$SCRIPT_DIR/src/lwfm/midware/_impl/SvcLauncher.py" > ~/.lwfm/logs/launcher.log 2>&1 &
    FLASK_PID=$!
    echo "$FLASK_PID" > ~/.lwfm/logs/midware.PID
    echo "lwfm service PID = $FLASK_PID"
    
    # Wait a moment for server to start
    sleep 2
fi



# trap INT and TERM signals to clean up - call the above function
trap cleanup INT TERM


# Script completed - services are running
echo "LWFM services started successfully"
echo "Flask server: http://127.0.0.1:3000"
echo "Use './lwfm.sh status' to check service status"
echo "Use './lwfm.sh gui' to launch GUI if needed" 

