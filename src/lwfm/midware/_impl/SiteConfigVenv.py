#pylint: disable=invalid-name, broad-exception-caught, broad-exception-raised
"""
Helper for venv handling 
"""

import os
import subprocess

from lwfm.base.Site import SitePillar, SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.midware._impl.ObjectSerializer import ObjectSerializer
from lwfm.midware._impl.SiteConfig import SiteConfig


class SiteConfigVenv():
    """
    Helper for venv handling 
    """

    def makeVenvPath(self, siteName: str) -> str:
        """
        Construct the path to the virtual environment used to run the Site driver.
        """
        props = SiteConfig.getSiteProperties(siteName)
        if props['venv']:
            return os.path.expanduser(props['venv'])
        return os.path.join(os.getcwd(), ".venv")



    def executeInProjectVenv(self, siteName: str, script_path_cmd: str = None) -> str:
        """
        Run a command in a virtual environment, used to run canonical Site methods.
        Arbitrary scripts can subsequently be run via Site.Run.submit().
        """
        if script_path_cmd is None:
            raise ValueError("script_path_cmd is required")

        venv_path = self.makeVenvPath(siteName)
        if os.name == "nt":    # windows
            python_executable = os.path.join(venv_path, "Scripts", "python.exe")
        else:   # a real OS
            python_executable = os.path.join(venv_path, "bin", "python")

        try:
            # Execute the command, making sure to capture only the last line as the
            # serialized return value
            # This will allow earlier prints to go to a file without disrupting the
            # return value
            modified_cmd = script_path_cmd
            if "driver.submit(" in script_path_cmd:
                # Modify script to make sure the serialized result is the only thing
                # printed at the end
                modified_cmd = script_path_cmd.replace("print(obj)",
                    "import sys; sys.stdout.write('RESULT_MARKER: ' + obj)")

            # execute a semicolon separated command in the virtual environment; this
            # includes an import statement and the method call
            process = subprocess.Popen([python_executable, "-c", modified_cmd],
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE,
                                        text=True
                                        )
            # read the output and error streams
            stdout, stderr = process.communicate()

            if process.returncode != 0:
                # something bad happened in the subprocess
                # we're going to give up the ghost - dump the stdout/err for debug
                print(stdout)
                print(stderr)
                raise RuntimeError("executeInProjectVenv: failed with code: " + \
                            f"{process.returncode}")

            if stdout:
                # Check if we marked the result
                if "RESULT_MARKER: " in stdout:
                    # Extract only the serialized part
                    result_lines = stdout.split("RESULT_MARKER: ")
                    return result_lines[-1].strip()
                # Otherwise return the whole output
                return stdout

            return None
        except Exception as ex:
            # something really bad happened
            raise Exception(f"executeInProjectVenv: an error occurred: {ex}") from ex


    def makeSerializeReturnString(self, argName: str = "obj") -> str:
        """
        Serialize an object into a string. This is used to pass objects back from the venv
        subprocess back to the main process (see discussion above).
        """
        # construct the command string to execute in the virtual environment
        return f"obj = lwfManager.serialize({argName}); " + \
            "print(obj)"


    # def _makeSiteNameCommandString(siteName: str) -> str:
    #     """
    #     Get the site name.
    #     """
    #     return "from lwfm.midware.LwfManager import lwfManager; " + \
    #            f"site = lwfManager.getSite('{siteName}'); "

    def makeSiteNameCommandString(self, siteName: str) -> str:
        """
        Create a command string to get the appropriate site implementation directly,
        avoiding the potential recursion from lwfManager.getSite().
        """
        baseSiteName = siteName
        if baseSiteName.endswith("-venv"):
            baseSiteName = baseSiteName[:-5]  # Remove "-venv" suffix
        return "from lwfm.midware.LwfManager import lwfManager; " + \
            "props = lwfManager.getSiteProperties('" + baseSiteName + "'); " + \
            "class_name = props['class'].split('.')[-1]; " + \
            "module_name = '.'.join(props['class'].split('.')[:-1]); " + \
            "site_module = __import__(module_name, fromlist=['*']); " + \
            "site_class = getattr(site_module, class_name); " + \
            f"site = site_class(); site.setSiteName('{baseSiteName}'); "


    def makeSiteDriverCommandString(self, sitePillar: SitePillar, siteName: str) -> str:
        """
        Import a class from a module and instantiate it.
        """
        # construct the command string to execute in the virtual environment
        driverStr = ""
        if isinstance(sitePillar, SiteAuth):
            driverStr = "driver = site.getAuthDriver()"
        elif isinstance(sitePillar, SiteRun):
            driverStr = "driver = site.getRunDriver()"
        elif isinstance(sitePillar, SiteRepo):
            driverStr = "driver = site.getRepoDriver()"
        elif isinstance(sitePillar, SiteSpin):
            driverStr = "driver = site.getSpinDriver()"
        else:
            driverStr = "driver = site.getAuthDriver()"
        return self.makeSiteNameCommandString(siteName) + \
            driverStr + "; "


    def makeArgWrapper(self, obj: object) -> str:
        """
        Any argument we want to pass to a Site method might be a complex object, so we
        serialize it to pass it to the subprocess running the venv.
        """
        return f"'{ObjectSerializer.serialize(obj)}'"
