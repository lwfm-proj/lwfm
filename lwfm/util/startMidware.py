"""
start the lwfm middleware
"""

from lwfm.midware.impl.LwfmEventSvc import app

def startMidware():
    """
    """
    app.run(host="0.0.0.0", port=3000)


if __name__ == "__main__":
    startMidware()
