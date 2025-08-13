"""
Workflow Tree Visualizer

A utility for building and displaying workflow job trees from lwfm workflow data.
Shows parent-child relationships between jobs in a visual tree format.
"""

import os

def build_workflow_tree(workflow_data):
    """
    Build a tree structure from workflow data showing job relationships.
    Returns a tree with workflow at root and jobs connected via parent relationships.
    """
    if not workflow_data or 'jobs' not in workflow_data:
        return None

    jobs = workflow_data['jobs']
    
    # Handle workflow info - it might be a string (workflowId) or a dict
    workflow_info = workflow_data.get('workflow', {})
    if isinstance(workflow_info, str):
        # If workflow is just a string, it's probably the workflowId
        workflow_id = workflow_info
        workflow_name = 'Unnamed Workflow'
    elif isinstance(workflow_info, dict):
        # If workflow is a dict, extract workflowId and name
        workflow_id = workflow_info.get('workflowId', 'unknown')
        workflow_name = workflow_info.get('name', 'Unnamed Workflow')
    else:
        # Fallback for any other type
        workflow_id = str(workflow_info) if workflow_info else 'unknown'
        workflow_name = 'Unnamed Workflow'
    
    # Build lookup maps
    job_lookup = {}  # jobId -> job_data
    children_map = {}  # parentId -> [child_jobs]
    root_jobs = []  # jobs with no parent
    
    for job in jobs:
        # Handle JobStatus objects - use methods instead of .get()
        if hasattr(job, 'getJobId'):
            job_id = job.getJobId()
            # Try different ways to get parent job ID
            parent_id = None
            if hasattr(job, 'getParentJobId'):
                parent_id = job.getParentJobId()
            elif hasattr(job, 'getJobContext') and job.getJobContext() and hasattr(job.getJobContext(), 'getParentJobId'):
                parent_id = job.getJobContext().getParentJobId()
            
            # Get command - try different approaches
            command = 'unknown'
            if hasattr(job, 'getCommand'):
                command = job.getCommand()
            elif hasattr(job, 'getJobDefn') and job.getJobDefn() and hasattr(job.getJobDefn(), 'getCommand'):
                command = job.getJobDefn().getCommand()
            
            status = job.getStatus() if hasattr(job, 'getStatus') else 'unknown'
        else:
            # Fallback for dict-like objects
            job_id = job.get('jobId')
            parent_id = job.get('parentJobId')
            command = job.get('command', 'unknown')
            status = job.get('status', 'unknown')
        
        if job_id:
            # Store the job with extracted data for easier access later
            job_data = {
                'jobId': job_id,
                'parentJobId': parent_id,
                'command': command,
                'status': status,
                'original': job  # Keep reference to original object if needed
            }
            job_lookup[job_id] = job_data
            
            if parent_id and parent_id != '' and parent_id != 'None':
                # This job has a parent
                if parent_id not in children_map:
                    children_map[parent_id] = []
                children_map[parent_id].append(job_data)
            else:
                # This job has no parent - connects directly to workflow root
                root_jobs.append(job_data)
    
    # Build the tree structure
    workflow_tree = {
        'type': 'workflow',
        'workflowId': workflow_id,
        'name': workflow_name,
        'children': []
    }
    
    def build_subtree(job_data):
        """Recursively build subtree for a job and its children"""
        job_id = job_data.get('jobId')
        subtree = {
            'type': 'job',
            'jobId': job_id,
            'command': job_data.get('command', 'unknown'),
            'status': job_data.get('status', 'unknown'),
            'parentJobId': job_data.get('parentJobId'),
            'children': []
        }
        
        # Add children if any
        if job_id in children_map:
            for child_job in children_map[job_id]:
                subtree['children'].append(build_subtree(child_job))
        
        return subtree
    
    # Add root jobs to workflow
    for root_job in root_jobs:
        workflow_tree['children'].append(build_subtree(root_job))
    
    return workflow_tree


def print_tree(node, indent=0, is_last=True, prefix=""):
    """
    Print the tree structure in a nice visual format using Unicode box characters.
    """
    # Determine the connector and next prefix
    if indent == 0:
        connector = ""
        next_prefix = ""
    else:
        connector = "└── " if is_last else "├── "
        next_prefix = prefix + ("    " if is_last else "│   ")
    
    # Print current node
    if node['type'] == 'workflow':
        print(f"{prefix}{connector}🔄 Workflow: [{node['workflowId']}]")
    else:
        if node['status'] == 'COMPLETE':
            status_icon = "✅"
        elif node['status'] == 'INFO':
            status_icon = "ℹ️"
        elif node['status'] in ('CANCELLED', 'FAILED'):
            status_icon = "❌"
        else:
            status_icon = "⏳"
        parent_info = f" (parent: {node['parentJobId']})" if node.get('parentJobId') else ""
        print(f"{prefix}{connector}{status_icon} Job: [{node['jobId']}]{parent_info}")
    
    # Print children
    children = node.get('children', [])
    for i, child in enumerate(children):
        is_last_child = (i == len(children) - 1)
        print_tree(child, indent + 1, is_last_child, next_prefix)


def display_workflow_tree(workflow_data):
    """
    Convenience function to build and display a workflow tree with metasheets.
    """
    tree = build_workflow_tree(workflow_data)
    if tree:
        print("\nWorkflow Tree Structure:")
        print_tree(tree)
        
        # Also display metasheets if available
        metasheets = workflow_data.get('metasheets', [])
        if metasheets:
            print(f"\n📄 Associated Metasheets ({len(metasheets)}):")
            for i, metasheet in enumerate(metasheets, 1):
                # Handle Metasheet objects - they always have methods, not dict access
                local_path = metasheet.getLocalUrl()
                # Extract just the filename part
                local_path = os.path.basename(local_path)
                job_id = metasheet.getJobId()
                props = metasheet.getProps()
                
                direction = props.get('_direction', 'unknown')
                direction_icon = "📤" if direction == 'put' else "📥" if direction == 'get' else "📄"
                
                print(f"   {i}. {direction_icon} {local_path} (job: {job_id})")
                
                # Show all metadata properties
                if props:
                    print(f"      └─ Props: {props}")
                else:
                    print("      └─ Props: (empty)")
        
        return True
    else:
        print("\nNo workflow tree could be built (no job data found)")
        return False
