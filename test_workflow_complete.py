#!/usr/bin/env python3
"""
Complete test of workflow dialog functionality
"""

import sys
import os
sys.path.insert(0, 'src')

import tkinter as tk
from lwfm.midware._impl.gui.workflow import open_workflow_dialog
from lwfm.midware.LwfManager import lwfManager

def test_workflow_data():
    """Test that workflow data can be loaded"""
    workflow_id = 'f253f59d'
    
    print("Testing workflow data loading...")
    try:
        wf = lwfManager.getWorkflow(workflow_id)
        if wf:
            print(f"✓ Workflow found: {wf.getName()}")
            print(f"✓ Description: {wf.getDescription()}")
            print(f"✓ Properties: {wf.getProps()}")
            return True
        else:
            print("✗ Workflow not found")
            return False
    except Exception as e:
        print(f"✗ Error loading workflow: {e}")
        return False

def main():
    # Test data loading first
    if not test_workflow_data():
        print("Workflow data test failed - cannot proceed with UI test")
        return
    
    print("\nOpening workflow dialog...")
    root = tk.Tk()
    root.title("Workflow Dialog Test")
    root.geometry("400x200")
    
    # Instructions
    instructions = tk.Label(root, 
        text="Click 'Open Workflow Dialog' to test the workflow overview.\n"
             "Check that the Overview tab shows:\n"
             "- Workflow ID: f253f59d\n"
             "- Name: A->B->C test\n"
             "- Description: A test of chaining...\n"
             "- Properties in JSON format",
        justify=tk.LEFT)
    instructions.pack(pady=10, padx=10)
    
    def open_dialog():
        try:
            open_workflow_dialog(root, 'f253f59d')
            print("✓ Workflow dialog opened successfully")
        except Exception as e:
            print(f"✗ Error opening dialog: {e}")
            import traceback
            traceback.print_exc()
    
    btn = tk.Button(root, text="Open Workflow Dialog", command=open_dialog, 
                   bg="lightblue", font=("Arial", 12))
    btn.pack(pady=10)
    
    quit_btn = tk.Button(root, text="Quit", command=root.quit, 
                        bg="lightcoral", font=("Arial", 10))
    quit_btn.pack(pady=5)
    
    print("✓ Test GUI ready - click the button to test the workflow dialog")
    root.mainloop()

if __name__ == "__main__":
    main()
