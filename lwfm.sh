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

# Background GUI launcher with logging and same fallback behavior
launch_gui_bg() {
    echo "Launching GUI in background... (logs: ~/.lwfm/logs/gui.log)"
    if [ "$(uname)" = "Linux" ] && [ -z "$DISPLAY" ]; then
        echo "Error: No DISPLAY variable set - GUI cannot launch" | tee -a ~/.lwfm/logs/gui.log
        echo "Try: export DISPLAY=:0 or run from a GUI session" | tee -a ~/.lwfm/logs/gui.log
        return 1
    fi

    # run in a subshell so we can nohup the whole retry logic
    nohup bash -lc '\
        echo "Using Python: '$PY'"; \
        echo "SCRIPT_DIR: '$SCRIPT_DIR'"; \
        "$PY" -m lwfm.midware._impl.gui || \
        PYTHONPATH="'$SCRIPT_DIR'/src${PYTHONPATH:+:$PYTHONPATH}" "$PY" -m lwfm.midware._impl.gui \
    ' > ~/.lwfm/logs/gui.log 2>&1 &
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
esac

# choose a Python 3 interpreter (prefer active venv, then python3, then python if it's Python 3)
pick_py3() {
    if [ -n "$VIRTUAL_ENV" ] && [ -x "$VIRTUAL_ENV/bin/python" ]; then
        echo "$VIRTUAL_ENV/bin/python"
        return 0
    fi
    if command -v python3 >/dev/null 2>&1; then
        echo "$(command -v python3)"
        return 0
    fi
    if command -v python >/dev/null 2>&1; then
        CANDIDATE="$(command -v python)"
        MAJOR="$($CANDIDATE - <<'PY'
import sys
print(sys.version_info[0])
PY
)"
        if [ "$MAJOR" = "3" ]; then
            echo "$CANDIDATE"
            return 0
        fi
    fi
    return 1
}

if PY=$(pick_py3); then
    :
else
    echo "Error: No suitable Python 3 interpreter found" >&2
    exit 1
fi

# Function to launch GUI
launch_gui() {
    echo "Launching GUI..."
    
    # Check if we have a display environment
    if [ "$(uname)" = "Linux" ] && [ -z "$DISPLAY" ]; then
        echo "Error: No DISPLAY variable set - GUI cannot launch"
        echo "Try: export DISPLAY=:0 or run from a GUI session"
        return 1
    fi
    
    echo "Using Python: $PY"
    echo "SCRIPT_DIR: $SCRIPT_DIR"
    # Try normal package launch (uses __main__.py to call main())
    if "$PY" -c "import lwfm.midware._impl.gui" 2>/dev/null; then
        # Module is importable, launch it
        "$PY" -m lwfm.midware._impl.gui
    else
        # Module not found, try with PYTHONPATH fallback
        echo "GUI module not found in standard path. Trying with PYTHONPATH fallback..." >&2
        PYTHONPATH="$SCRIPT_DIR/src${PYTHONPATH:+:$PYTHONPATH}" "$PY" -m lwfm.midware._impl.gui
    fi
}

# Handle GUI command after Python interpreter is set
case "${1:-}" in
    "gui")
        launch_gui
        exit 0
        ;;
esac

# Check if Flask server is already running
if check_flask_running; then
    echo "Flask server is already running on port 3000"
    FLASK_PID=$(pgrep -f "SvcLauncher.py" | head -1)
    if [ -n "$FLASK_PID" ]; then
        echo "Found existing Flask PID = $FLASK_PID"
    else
        echo "Warning: Flask server responding but PID not found"
    fi
else
    echo "Starting Flask server..."
    # start with a clean log only when starting a new server session
    : > ~/.lwfm/logs/midware.log
    : > ~/.lwfm/logs/launcher.log
    # launch the middleware in the background and route stdout and stderr to a log file
    "$PY" "$SCRIPT_DIR/src/lwfm/midware/_impl/SvcLauncher.py" > ~/.lwfm/logs/launcher.log 2>&1 &
    FLASK_PID=$!
    echo "lwfm service PID = $FLASK_PID"
    
    # Wait a moment for server to start
    sleep 2
fi

# Launch GUI only if not called with 'gui' argument (avoid duplicate launch)
if [ "${1:-}" != "gui" ]; then
    launch_gui
fi


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
    rm -f ~/.lwfm/logs/midware.pid 2>/dev/null
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


# Script completed - services are running
echo "LWFM services started successfully"
echo "Flask server: http://127.0.0.1:3000"
echo "Use './lwfm.sh status' to check service status"
echo "Use './lwfm.sh gui' to launch GUI if needed" 

