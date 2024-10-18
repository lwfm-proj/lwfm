from lwfm.base.JobDefn import JobDefn
from lwfm.base.Site import Site


import unittest

class TestRunJobsSynchronously(unittest.TestCase):
    def test_run_jobs_synchronously(self):
        """
        'use @fn:submitJob() (Site.py:86:5-128:13) and @fn:wait() (JobStatus.py:280:5-294:22) to write a unittest case to run an echo job after another similar job completes'
        'in this case, the agent wanted guidance on the submitJob() and wait() methods'
        'it also needed to be reminded about getRunDriver()'
        'for the second job, it suggested a waitFor() method'
        'it also suggested an 'isCompleted()' as better wording than 'isTerminalSuccess()'

        Test running jobs synchronously.

        Creates a local site instance and submits two jobs sequentially. 
        The first job is created with an entry point of "echo Job 1 output" and is submitted. 
        The status of the first job is then checked to ensure it has completed successfully. 
        The second job is created with an entry point of "echo Job 2 output" and is submitted after the first job completes. 
        The status of the second job is then checked to ensure it has completed successfully.

        Parameters:
        - self: The instance of the test class.

        Returns:
        - None
        """
        # Create a local site instance
        site = Site.getSiteInstanceFactory("local")

        # Create the first job
        job1 = JobDefn()
        job1.setEntryPoint("echo Job 1 output")

        # Submit the first job
        status1 = site.getRunDriver().submitJob(job1)

        # Wait for the first job to complete
        status1 = status1.wait()
        # print the status
        print(status1)

        # Check if the first job has completed successfully
        self.assertTrue(status1.isTerminalSuccess(), "Job 1 should have completed successfully")

        # Create the second job
        job2 = JobDefn()
        job2.setEntryPoint("echo Job 2 output")

        # Submit the second job after the first job completes
        status2 = site.getRunDriver().submitJob(job2)

        # Wait for the second job to complete
        status2 = status2.wait()
        # print the status 
        print(status2)

        # Check if the second job has completed successfully
        self.assertTrue(status2.isCompleted(), "Job 2 should have completed successfully")

if __name__ == '__main__':
    unittest.main()
