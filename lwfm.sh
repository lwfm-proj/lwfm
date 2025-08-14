#!/bin/bash
# Start the lwfm middleware - a REST service, a remote site poller, a DB maybe. 
# See the SvcLauncher for details.

# ~/.lwfm will contain config files and logs
# create if it doesn't exist
mkdir -p ~/.lwfm
mkdir -p ~/.lwfm/logs

touch ~/.lwfm/logs/midware.log
# start with a clean log for this session so old errors don't appear in tail
: > ~/.lwfm/logs/midware.log
# also clear launcher log
: > ~/.lwfm/logs/launcher.log

export PYTHONUNBUFFERED=1

# choose a Python interpreter (prefer active venv, then python, then python3)
if [ -n "$VIRTUAL_ENV" ] && [ -x "$VIRTUAL_ENV/bin/python" ]; then
    PY="$VIRTUAL_ENV/bin/python"
elif command -v python >/dev/null 2>&1; then
    PY="$(command -v python)"
elif command -v python3 >/dev/null 2>&1; then
    PY="$(command -v python3)"
else
    echo "Error: No Python interpreter found (need python or python3)" >&2
    exit 1
fi

# launch the middleware in the background and route stdout and stderr to a log file
"$PY" ./src/lwfm/midware/_impl/SvcLauncher.py > ~/.lwfm/logs/launcher.log 2>&1 &
FLASK_PID=$!
echo lwfm service PID = $FLASK_PID

# Do not launch GUI here; SvcLauncher will autostart GUI if available and enabled
GUI_PID=


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
    if [ -n "$GUI_PID" ]; then
        kill -TERM $GUI_PID 2>/dev/null
    fi
    # belt-and-suspenders: if middleware pid file exists, kill that session too
    if [ -f ~/.lwfm/logs/midware.pid ]; then
        MPID=$(cat ~/.lwfm/logs/midware.pid 2>/dev/null)
        if [ -n "$MPID" ]; then
            kill -TERM -- -$MPID 2>/dev/null
            sleep 2
            if ps -p $MPID > /dev/null; then
                kill -KILL -- -$MPID 2>/dev/null
            fi
        fi
    fi
    # remove pid files if present
    rm -f ~/.lwfm/logs/midware.pid ~/.lwfm/logs/gui.pid 2>/dev/null
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

# trap INT and TERM signals to clean up - call the above function
trap cleanup INT TERM


# tail the log continuously, until control-c
tail -f ~/.lwfm/logs/midware.log 

