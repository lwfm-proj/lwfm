#!/usr/bin/env python3
"""
Simple GUI test to verify Tkinter works and GUI module can be imported.
"""
import sys
import os
sys.path.insert(0, 'src')

print("Testing basic Tkinter...")
try:
    import tkinter as tk
    root = tk.Tk()
    root.title("Test Window")
    root.geometry("300x200")
    tk.Label(root, text="Tkinter works!", font=("Arial", 16)).pack(pady=50)
    root.after(3000, root.quit)  # Auto-close after 3 seconds
    root.mainloop()
    print("✓ Basic Tkinter test passed")
except Exception as e:
    print(f"✗ Tkinter test failed: {e}")
    sys.exit(1)

print("\nTesting LWFM GUI import...")
try:
    from lwfm.midware._impl.gui.app import LwfmGui
    print("✓ GUI module import successful")
except Exception as e:
    print(f"✗ GUI import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\nTesting LWFM GUI creation...")
try:
    app = LwfmGui()
    print("✓ GUI instance created")
    app.after(3000, app.quit)  # Auto-close after 3 seconds
    app.mainloop()
    print("✓ GUI test completed successfully")
except Exception as e:
    print(f"✗ GUI creation failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\nAll GUI tests passed!")
