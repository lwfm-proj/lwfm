#!/bin/bash
# Start the lwfm middleware - a REST service, a remote site poller, a DB maybe. 
# See the SvcLauncher for details.

# ~/.lwfm will contain config files and logs
# create if it doesn't exist
mkdir -p ~/.lwfm
mkdir -p ~/.lwfm/logs

touch ~/.lwfm/logs/midware.log

export PYTHONUNBUFFERED=1

# launch the middleware in the background and route stdout and stderr to a log file
python ./src/lwfm/midware/_impl/SvcLauncher.py > ~/.lwfm/logs/launcher.log 2>&1 &
FLASK_PID=$!
echo lwfm service PID = $FLASK_PID

# launch the GUI in the background (logs to ~/.lwfm/logs/gui.log)
# launch the GUI in the background (logs to ~/.lwfm/logs/gui.log) if tkinter is available
if python - <<'PY'
import importlib, sys
try:
    importlib.import_module('tkinter')
    sys.exit(0)
except Exception:
    sys.exit(1)
PY
then
    python ./src/lwfm/midware/_impl/gui/run_gui.py > ~/.lwfm/logs/gui.log 2>&1 &
    GUI_PID=$!
    echo lwfm GUI PID = $GUI_PID
else
    echo "Tkinter not available in current Python; skipping GUI launch. See README for setup." >&2
    GUI_PID=
fi


# function to clean up background processes
cleanup() {
    echo " * Caught exit signal - propagating... "
    if [ -n "$FLASK_PID" ]; then
        kill -SIGINT -- -$FLASK_PID 2>/dev/null
        sleep 5
        if ps -p $FLASK_PID > /dev/null; then
            kill -9 -- -$FLASK_PID 2>/dev/null
        fi
    fi
    if [ -n "$GUI_PID" ]; then
        kill -TERM $GUI_PID 2>/dev/null
    fi
    # rotate the log file for safe keeping 
    mv ~/.lwfm/logs/midware.log ~/.lwfm/logs/midware-$FLASK_PID.log
    mv ~/.lwfm/logs/launcher.log ~/.lwfm/logs/launcher-$FLASK_PID.log
    if [ -f ~/.lwfm/logs/gui.log ]; then
        mv ~/.lwfm/logs/gui.log ~/.lwfm/logs/gui-$GUI_PID.log
    fi
    echo " * DONE"
    exit 0
}

# trap INT and TERM signals to clean up - call the above function
trap cleanup INT # TERM


# tail the log continuously, until control-c
tail -f ~/.lwfm/logs/midware.log 

