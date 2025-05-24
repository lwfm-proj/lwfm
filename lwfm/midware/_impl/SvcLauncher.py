"""
start the lwfm middleware
"""

#pylint: disable = invalid-name, broad-exception-caught, unused-argument, protected-access

import signal
import sys
import os
import subprocess
import threading
import time
import requests

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
            print(f"isMidwareRunning(1): {url}")
            return False
        except requests.exceptions.ConnectionError:
            # Connection error means server is not running
            print(f"isMidwareRunning(2): {url} failed to connect")
            return False
        except requests.exceptions.RequestException as ex:
            # Handle other request-related errors (timeout, invalid URL, etc.)
            print(f"isMidwareRunning(3): {url} request error: {str(ex)}")
            return False
        except Exception as ex:
            # Catch any other unexpected exceptions
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
                    print(f"Server ready after {time.time() - start_time:.1f} seconds")
                    return True
            except requests.exceptions.ConnectionError:
                # Expected during startup - server not accepting connections yet
                pass
            except Exception as ex:
                # Log other unexpected errors but keep trying
                print(f"Unexpected error checking server: {str(ex)}")
            time.sleep(interval)
        return False


    #********************************************************************************

    def _startMidware(self):
        """
        Start the middleware either synchronously or asynchronously.
        """

        global _middleware_lock, _starting_middleware, _middleware_process

        # Prevent multiple concurrent starts
        with _middleware_lock:
            if _starting_middleware:
                return 0
            _starting_middleware = True

        print("*** starting lwfm service")

        # The server sees itself as 0.0.0.0. The port can be set in lwfm sites.toml or
        # use the default.
        # (The address the client sees can also be set in the toml - see below.)
        SERVER_HOST = "0.0.0.0"
        PORT = 3000

        # Get lwfm server port from configuration
        lwfm_config = SiteConfig.getSiteProperties("lwfm")
        port = lwfm_config.get("port") if lwfm_config else PORT
        clientHost = lwfm_config.get("host") if lwfm_config else "127.0.0.1"

        # Server URL, as seen by clients (LwfmEventClient)
        server_url = f"http://{clientHost}:{port}"

        print(f"*** client will use {server_url}")

        if SvcLauncher.isMidwareRunning(server_url):
            # the instance of the middleware is already running
            print(f"*** lwfm server is already running at {server_url} - halting this launch")
            with _middleware_lock:
                _starting_middleware = False
            return 0

        try:
            print(f"*** lwfm server not already running - proceeding with launch")

            project_root = os.path.abspath(os.getcwd())

            flask_script = f"""
import sys
import os
import traceback
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(sys.stderr)])

logger = logging.getLogger('flask_app')

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
        app.run(host='{SERVER_HOST}', port={port})
    except ImportError as e:
        logger.error("Import error: %s", e)
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
except Exception as e:
    logger.error("Unexpected error: %s", e)
    traceback.print_exc(file=sys.stderr)
    sys.exit(1)
"""

            print("*** launch cmd: " + flask_script)

            log_file_path = os.path.expanduser(SiteConfig.getLogFilename() + "/midware.log")
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

            # Run the Flask app in a subprocess (asynchronously) in detached mode
            _middleware_process = subprocess.Popen(
                [sys.executable, '-c', flask_script],
                stdout=open(log_file_path, 'a'),
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                # Add these lines to ensure proper environment inheritance
                env=os.environ.copy(),
                start_new_session=True  # Important for proper process group handling
            )

            print(f"*** lwfm server process started, PID: {_middleware_process.pid}")
            print("*** allowing time for initialization...")
            # Try to ensure server is actually ready
            if not self._wait_for_server_ready(server_url, timeout=30):
                print("*** Warning: lwfm server may not be fully initialized")
            else:
                # Additional wait time to ensure the server is fully ready
                time.sleep(10)  # Extra seconds wait after server reports ready
                print("*** lwfm server initialization wait complete")
                print("*** READY to process jobs.")

            return _middleware_process
        finally:
            with _middleware_lock:
                _starting_middleware = False


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
