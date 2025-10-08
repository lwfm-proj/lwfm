"""
Custom exceptions for the LWFM framework.
"""

class JobNotFoundException(Exception):
    """
    Raised when a job cannot be found on a site.
    This typically indicates the job has completed and been purged from the remote system.
    """
    def __init__(self, job_id: str, site_name: str = None):
        self.job_id = job_id
        self.site_name = site_name
        if site_name:
            message = f"Job not found: {job_id} on site {site_name}"
        else:
            message = f"Job not found: {job_id}"
        super().__init__(message)
