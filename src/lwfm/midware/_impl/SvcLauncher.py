"""
start the lwfm middleware
"""

#pylint: disable = invalid-name, broad-exception-caught, unused-argument, protected-access
#pylint: disable = global-statement, global-variable-not-assigned

import signal
import sys
import os
import subprocess
import threading
import time
import requests
import textwrap
try:
    import fcntl  # for inter-process file locking on Unix/macOS
except Exception:  # pragma: no cover - platform without fcntl
    fcntl = None

from lwfm.midware._impl.SiteConfig import SiteConfig

# try and limit concurrency - we only need one instance of the services running on
# a published host/port. the SiteConfig contains the host and port, and if not, we
# use localhost and a known port.

# semaphore for locking
_middleware_lock = threading.Lock()
# we lock this variable
_starting_middleware = False
# track the instance of the REST service - there are other processes, but we track this one
_middleware_process = None
_gui_process = None


class SvcLauncher:
    """
    Launch the lwfm middleware.
    """

    # a separate process to handle asynchronous events, poll remote sites, etc.
    _event_Processor = None

    @staticmethod
    def isMidwareRunning(url : str) -> bool:
        """
        Check if the middleware services are running.
        """
        try:
            response = requests.get(f"{url}/isRunning", timeout=10)
            if response.ok:
                return True
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print(f"isMidwareRunning(1): {url}")
            return False
        except requests.exceptions.ConnectionError:
            # Connection error means server is not running
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print(f"isMidwareRunning(2): {url} failed to connect")
            return False
        except requests.exceptions.RequestException as ex:
            # Handle other request-related errors (timeout, invalid URL, etc.)
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print(f"isMidwareRunning(3): {url} request error: {str(ex)}")
            return False
        except Exception as ex:
            # Catch any other unexpected exceptions
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print(f"isMidwareRunning(4): {url} unexpected error: {str(ex)}")
            return False


    #********************************************************************************

    # Signal handler for Ctrl+C
    @staticmethod
    def _signal_handler(sig, frame):
        """
        Handle Ctrl+C signal by terminating the middleware process
        """
        print(f"*** lwfm server exit signal={sig} handler invoked")

        # we don't expect to see much of the print statements after this...

        global _middleware_process

        if _middleware_process:
            try:
                # Send SIGTERM to the process group
                print(f"*** Sending SIGTERM to process group {_middleware_process.pid}")
                os.killpg(os.getpgid(_middleware_process.pid), signal.SIGTERM)

                # Wait for process to terminate
                print("*** Waiting for process to terminate...")
                _middleware_process.wait(timeout=5)

            except subprocess.TimeoutExpired:
                print("*** Process did not terminate gracefully, forcing...")
                os.killpg(os.getpgid(_middleware_process.pid), signal.SIGKILL)

            except ProcessLookupError:
                print("*** Process already terminated")

            _middleware_process = None

        # Re-raise the signal to exit
        signal.signal(sig, signal.SIG_DFL)
        os.kill(os.getpid(), sig)


    #********************************************************************************

    def _wait_for_server_ready(self, url, timeout=30, interval=0.5):
        """Wait for server to be ready to accept connections."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                if SvcLauncher.isMidwareRunning(url):
                    if os.getenv("LWFM_VERBOSE", "0") == "1":
                        print(f"Server ready after {time.time() - start_time:.1f} seconds")
                    return True
            except requests.exceptions.ConnectionError:
                # Expected during startup - server not accepting connections yet
                pass
            except Exception as ex:
                # Log other unexpected errors but keep trying
                if os.getenv("LWFM_VERBOSE", "0") == "1":
                    print(f"Unexpected error checking server: {str(ex)}")
            time.sleep(interval)
        return False


    #********************************************************************************

    def _startMidware(self):
        """
        Start the middleware either synchronously or asynchronously.
        """
        global _middleware_lock, _starting_middleware, _middleware_process
        lock_file = None

        # Prevent multiple concurrent starts within this process
        with _middleware_lock:
            if _starting_middleware:
                return 0
            _starting_middleware = True
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print("*** starting lwfm service")

        # The server listens on 0.0.0.0, port may be overridden in config
        SERVER_HOST = "0.0.0.0"
        PORT = 3000

        # Get lwfm server port/host from configuration
        lwfm_config = SiteConfig.getSiteProperties("lwfm")
        port = lwfm_config.get("port") if lwfm_config else PORT
        clientHost = lwfm_config.get("host") if lwfm_config else "127.0.0.1"

        # Server URL, as seen by clients (LwfmEventClient)
        server_url = f"http://{clientHost}:{port}"

        if os.getenv("LWFM_VERBOSE", "0") == "1":
            print(f"*** client will use {server_url}")

        # If already running, skip launch
        if SvcLauncher.isMidwareRunning(server_url):
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print(f"*** lwfm server is already running at {server_url} - halting this launch")
            with _middleware_lock:
                _starting_middleware = False
            return 0

        # Acquire an inter-process startup lock to avoid race conditions between multiple starters
        try:
            lock_dir = os.path.expanduser(SiteConfig.getLogFilename())
            os.makedirs(lock_dir, exist_ok=True)
            lock_path = os.path.join(lock_dir, "midware.start.lock")
            lock_file = open(lock_path, "a+", encoding="utf-8")
            if fcntl is not None:
                # Try non-blocking first with brief retries so we can re-check server status while waiting
                deadline = time.time() + 20.0  # seconds
                while True:
                    try:
                        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        # Got the lock
                        break
                    except BlockingIOError:
                        # Another process is starting; if server comes up, abort
                        if SvcLauncher.isMidwareRunning(server_url):
                            if os.getenv("LWFM_VERBOSE", "0") == "1":
                                print("*** Another process started lwfm server while waiting for lock; skipping launch")
                            with _middleware_lock:
                                _starting_middleware = False
                            try:
                                lock_file.close()
                            except Exception:
                                pass
                            return 0
                        if time.time() >= deadline:
                            # As a last resort, block until we can take the lock
                            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                            break
                        time.sleep(0.25)
        except Exception as ex:
            # If locking fails, proceed without it but log when verbose
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print(f"*** Startup lock unavailable: {ex}")
            try:
                if lock_file:
                    lock_file.close()
            except Exception:
                pass
            lock_file = None

        try:
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print("*** lwfm server not already running - proceeding with launch")

            project_root = os.path.abspath(os.getcwd())

            flask_script = textwrap.dedent(f"""
                import sys
                import os
                import traceback
                import logging

                # Set up logging
                logging.basicConfig(
                    level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stderr)]
                )

                logger = logging.getLogger('flask_app')

                def main():
                    try:
                        # Set the absolute path to the project
                        sys.path.insert(0, '{project_root}')
                        logger.info("Python executable: %s", sys.executable)
                        logger.info("Working directory: %s", os.getcwd())
                        logger.info("Python path: %s", sys.path)

                        try:
                            from lwfm.midware._impl.LwfmEventSvc import app
                            logger.info("Flask app imported successfully")
                            logger.info("Starting Flask app on {SERVER_HOST}:{port}")
                            app.run(host='{SERVER_HOST}', port={port}, use_reloader=False)
                        except ImportError as e:
                            logger.error("Import error: %s", e)
                            traceback.print_exc(file=sys.stderr)
                            sys.exit(1)
                    except Exception as e:
                        logger.error("Unexpected error: %s", e)
                        traceback.print_exc(file=sys.stderr)
                        sys.exit(1)

                if __name__ == '__main__':
                    main()
            """)

            # Do not print the inline script; it's noisy for users
            log_file_path = os.path.expanduser(SiteConfig.getLogFilename() + "/midware.log")
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

            # Run the Flask app in a subprocess (asynchronously) in detached mode
            with open(log_file_path, 'a', encoding='utf-8') as log_file:
                env = os.environ.copy()
                env["LWFM_SERVER"] = "1"
                _middleware_process = subprocess.Popen(
                    [sys.executable, '-c', flask_script],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    env=env,
                    start_new_session=True  # Important for proper process group handling
                )
            # Write middleware PID file for belt-and-suspenders cleanup
            try:
                pid_dir = os.path.expanduser(SiteConfig.getLogFilename())
                os.makedirs(pid_dir, exist_ok=True)
                with open(os.path.join(pid_dir, 'midware.pid'), 'w', encoding='utf-8') as pf:
                    pf.write(str(_middleware_process.pid))
            except Exception:
                pass
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print(f"*** lwfm server process started, PID: {_middleware_process.pid}")
                print("*** allowing time for initialization...")
            # Try to ensure server is actually ready
            if not self._wait_for_server_ready(server_url, timeout=30):
                if os.getenv("LWFM_VERBOSE", "0") == "1":
                    print("*** Warning: lwfm server may not be fully initialized")
            else:
                # Additional wait time to ensure the server is fully ready
                time.sleep(10)  # Extra seconds wait after server reports ready
                if os.getenv("LWFM_VERBOSE", "0") == "1":
                    print("*** lwfm server initialization wait complete")
                    print("*** READY to process jobs.")
            # Optionally start GUI after server launch if enabled
            # try:
            #     self._startGui()
            # except Exception as ex:
            #     if os.getenv("LWFM_VERBOSE", "0") == "1":
            #         print(f"*** GUI autostart failed or skipped: {ex}")
            return _middleware_process
        finally:
            # Release inter-process lock
            try:
                if lock_file is not None and fcntl is not None:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                if lock_file is not None:
                    lock_file.close()
            except Exception:
                pass
            with _middleware_lock:
                _starting_middleware = False

    def _startGui(self):
        """Start the Tk GUI in a detached background process if possible."""
        global _gui_process

        # Allow users to disable GUI autostart
        if os.getenv("LWFM_GUI_AUTOSTART", "1") != "1":
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print("*** GUI autostart disabled via LWFM_GUI_AUTOSTART=0")
            return 0

        # Check Tk availability in this interpreter
        try:
            import importlib
            importlib.import_module('tkinter')
        except Exception:
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print("*** Tkinter not available; skipping GUI autostart")
            return 0

        # Avoid duplicate GUI instances using a pid file
        gui_log_dir = os.path.expanduser(SiteConfig.getLogFilename())
        os.makedirs(gui_log_dir, exist_ok=True)
        gui_log_path = os.path.join(gui_log_dir, 'gui.log')
        pid_file = os.path.join(gui_log_dir, 'gui.pid')

        # If pid file exists and process alive, skip
        try:
            if os.path.exists(pid_file):
                with open(pid_file, 'r', encoding='utf-8') as pf:
                    existing_pid_str = pf.read().strip()
                if existing_pid_str.isdigit():
                    existing_pid = int(existing_pid_str)
                    try:
                        # Signal 0 just checks existence/permission
                        os.kill(existing_pid, 0)
                        if os.getenv("LWFM_VERBOSE", "0") == "1":
                            print(f"*** GUI already running with PID {existing_pid}; skipping autostart")
                        return 0
                    except Exception:
                        # Stale pid file; continue to start
                        pass
        except Exception:
            pass

        # Compute path to run_gui.py relative to this file
        here = os.path.dirname(os.path.abspath(__file__))
        gui_script = os.path.join(here, 'gui', 'run_gui.py')
        if not os.path.exists(gui_script):
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print(f"*** GUI launcher not found at {gui_script}; skipping GUI autostart")
            return 0

        try:
            with open(gui_log_path, 'a', encoding='utf-8') as log_file:
                env = os.environ.copy()
                # Do not mark this as server; it's a client UI
                env.pop('LWFM_SERVER', None)
                _gui_process = subprocess.Popen(
                    [sys.executable, gui_script],
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    env=env,
                    start_new_session=True
                )
            # Write out PID file
            try:
                with open(pid_file, 'w', encoding='utf-8') as pf:
                    pf.write(str(_gui_process.pid))
            except Exception:
                pass
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print(f"*** lwfm GUI process started, PID: {_gui_process.pid}")
            return _gui_process
        except Exception as ex:
            if os.getenv("LWFM_VERBOSE", "0") == "1":
                print(f"*** Failed to start GUI: {ex}")
            return 0


#********************************************************************************

def launchMidware():
    """
    run the services, register shutdown handler, keep alive
    """
    print("*** lwfm Svclauncher: main()")
    signal.signal(signal.SIGINT, SvcLauncher._signal_handler)
    signal.signal(signal.SIGTERM, SvcLauncher._signal_handler)

    proc = SvcLauncher()._startMidware()
    if proc is not None and proc != 0:
        # Keep the main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("*** lwfm server exit signal received")
            sys.exit(0)
    else:
        # nothing to do
        sys.exit(0)


#********************************************************************************


if __name__ == "__main__":
    launchMidware()
