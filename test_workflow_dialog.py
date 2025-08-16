#!/usr/bin/env python3
"""
Test workflow dialog display
"""

import sys
import os
sys.path.insert(0, 'src')

import tkinter as tk
from lwfm.midware._impl.gui.workflow import open_workflow_dialog

def main():
    root = tk.Tk()
    root.title("Test Workflow Dialog")
    root.geometry("300x100")
    
    def open_dialog():
        open_workflow_dialog(root, 'f253f59d')
    
    btn = tk.Button(root, text="Open Workflow Dialog", command=open_dialog)
    btn.pack(pady=20)
    
    root.mainloop()

if __name__ == "__main__":
    main()
