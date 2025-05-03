"""
start the lwfm middleware
"""

#pylint: disable = invalid-name

from lwfm.midware._impl.LwfmEventSvc import app

def startMidware():
    """
    Run the Flask app for lwfm middleware.
    """
    app.run(host="0.0.0.0", port=3000)


if __name__ == "__main__":
    startMidware()
