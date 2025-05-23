"""
Is the lwfm middleware running?
"""


from lwfm.midware.LwfManager import lwfManager

if __name__ == "__main__":
    print(f"*** Is the lwfm service running? {lwfManager.isMidwareRunning()}")

