#!/usr/bin/env python3
"""
Simple test of workflow dialog
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

import tkinter as tk
from lwfm.midware.LwfManager import lwfManager

def test_workflow_display():
    """Test workflow data display directly"""
    workflow_id = 'f253f59d'
    
    # Test data loading
    try:
        wf = lwfManager.getWorkflow(workflow_id)
        wf_name = wf.getName() if wf else "(not found)"
        wf_desc = wf.getDescription() if wf else "(not found)"
        wf_props = wf.getProps() if wf else {}
        
        print(f"Workflow ID: {workflow_id}")
        print(f"Name: '{wf_name}'")
        print(f"Description: '{wf_desc}'")
        print(f"Properties: {wf_props}")
        
        # Test UI display
        root = tk.Tk()
        root.title("Test Workflow Display")
        
        # Create labels exactly like in the dialog
        frame = tk.Frame(root)
        frame.pack(padx=20, pady=20)
        
        tk.Label(frame, text="Workflow ID:").grid(row=0, column=0, sticky=tk.W)
        tk.Entry(frame, value=workflow_id, width=60, state="readonly").grid(row=0, column=1, sticky=tk.W, padx=(4, 16))
        
        tk.Label(frame, text="Name:").grid(row=1, column=0, sticky=tk.W)
        name_label = tk.Label(frame, text=wf_name, wraplength=400)
        name_label.grid(row=1, column=1, sticky=tk.W, padx=(4, 16))
        
        tk.Label(frame, text="Description:").grid(row=2, column=0, sticky=tk.W)
        desc_label = tk.Label(frame, text=wf_desc, wraplength=400)
        desc_label.grid(row=2, column=1, sticky=tk.W, padx=(4, 16))
        
        print(f"Name label text: '{name_label.cget('text')}'")
        print(f"Desc label text: '{desc_label.cget('text')}'")
        
        root.after(3000, root.quit)
        root.mainloop()
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_workflow_display()
