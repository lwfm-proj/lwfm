"""
A set of examples showcasing the data handling capabilities of lwfm.
"""

#pylint: disable=invalid-name

import sys
import json
import tempfile
import os

from lwfm.base.Workflow import Workflow
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus
from lwfm.base.WorkflowEvent import JobEvent, MetadataEvent
from lwfm.base.JobContext import JobContext

from lwfm.midware.LwfManager import lwfManager


if __name__ == "__main__":

    # ****************************************************************************
    # workflow example

    # make a workflow, give it a name, save it
    wf = Workflow("lwfm data example")
    if (wf := lwfManager.putWorkflow(wf)) is None:
        sys.exit("Failed to put workflow")

    # give it some metadata properties, save it again
    someDict = {"metaprop1": "value1", "metaprop2": "value2"}
    wf.setProps(someDict)
    if (wf := lwfManager.putWorkflow(wf)) is None:
        sys.exit("Failed to put workflow")

    # update some properties and write some new ones, save it again
    props = wf.getProps()
    props.update({"metaprop2": "newvalue2", "metaprop3": "value3"})
    wf.setProps(props)
    if (wf := lwfManager.putWorkflow(wf)) is None:
        sys.exit("Failed to put workflow")
    # show we can read it back
    if (wf := lwfManager.getWorkflow(wf.getWorkflowId())) is None:
        sys.exit("Failed to get workflow")
    print(f"\n* out1: Workflow {wf.getWorkflowId()} has props: {wf.getProps()}")


    # ****************************************************************************
    # jobs in a workflow example - run a trivial job on the local site and associate it
    # with the workflow

    site = lwfManager.getSite("local")
    jobDefn = JobDefn("echo hello world")

    job = site.getRunDriver().submit(jobDefn)
    job = lwfManager.wait(job.getJobId())
    print(f"\n* out2: {job}")                            # job will have its own workflow

    job = site.getRunDriver().submit(jobDefn, wf)
    job = lwfManager.wait(job.getJobId())
    print(f"\n* out3: {job}")                            # job will be associated with the workflow


    # ****************************************************************************
    # jobs with associated data

    # Create a job that writes output to a file, then store that file in the repo
    temp_file = os.path.join(tempfile.gettempdir(), "hello_output.txt")
    jobDefn = JobDefn(f"echo 'hello world from job' > {temp_file}")
    job = site.getRunDriver().submit(jobDefn, wf)
    job = lwfManager.wait(job.getJobId())

    # Now put the file to the repo with the current context using site repo driver to put the file
    metasheet = site.getRepoDriver().put(temp_file, "hello_output.txt", job.getJobContext(),
        {
            "description": "Hello world output file",
            "file_type": "text",
            "created_by": "ex10_data_showcase",
            "foo": "bar",
        }
    )
    print(f"\n* out4: File stored in repo with metadata: {metasheet} - notice own job")

    # Do that again, but this time since the file is already local, just notate it.
    metasheet = lwfManager.notatePut(temp_file, wf.getWorkflowId(), {
        "description": "Hello world output file",
        "file_type": "text",
        "created_by": "ex10_data_showcase",
        "foo": "fooless"
    })
    print(f"\n* out5: File notated in repo with metadata: {metasheet} - notice own job")

    # Do it again, but make it causal
    job = site.getRunDriver().submit(jobDefn, wf)
    print(f"\n* out6: {job}")
    trigger_job_defn = JobDefn("repo.put", JobDefn.ENTRY_TYPE_SITE,
                               [temp_file, "hello_output_triggered.txt"])
    trigger_status = lwfManager.setEvent(JobEvent(job.getJobId(), JobStatus.COMPLETE,
                                                  trigger_job_defn, "local"))
    if trigger_status is None:
        sys.exit("Failed to set job trigger")
    trigger_status = lwfManager.wait(trigger_status.getJobId())
    print(f"\n* out7: Triggered job completed: {trigger_status} - notice the parent is above")


    # ****************************************************************************
    # gets can also be notated

    # Use repo.get to retrieve a well-known Unix file and copy it to /tmp with a new name
    wfContext = JobContext()
    wfContext.setWorkflowId(wf.getWorkflowId())
    destPath = site.getRepoDriver().get("/etc/passwd", "/tmp/system_users.txt", wfContext)
    print(f"\n* out8: {destPath}")


    # ****************************************************************************
    # find repo objects by their metadata - note this function is provided by the lwfManager
    # not the site, as the site might not have any concept...

    # clauses are ANDed together, wildcards permitted
    metasheets = lwfManager.find({"file_type": "t*", "foo": "fooless"})
    if not metasheets:
        print("\n* out9: No metasheets found with the given metadata")
    else:
        for sheet in metasheets:
            print(f"\n* out9: Found metasheet: {sheet}")


    # *****************************************************************************
    # similarly we can find workflows by their metadata, clauses also AND
    # lwfm tries to keep data history, so actually there is a record for each time
    # the workflow was saved, but this find will return the latest version

    workflows = lwfManager.findWorkflows({"metaprop1": "value1"})
    if not workflows:
        print("\n* out10: No workflows found with the given metadata")
    else:
        for wf in workflows:
            print(f"\n* out10: Found workflow: {wf.getWorkflowId()} with props: {wf.getProps()}")


    # *****************************************************************************
    # data triggers - when some data with a certain metadata profile / search clause
    # is "put", a job can be triggered to run

    # create a job that will be triggered by data put with a certain metadata
    sample_id = lwfManager.generateId()
    trigger_job_defn = JobDefn("echo hello world from data trigger")
    trigger_job = lwfManager.setEvent(
        MetadataEvent({"sampleId": sample_id}, trigger_job_defn, "local", None, wfContext)
    )
    # now put a file somewhere and notate it with our triggering metadata
    site.getRepoDriver().put("/etc/passwd", "/tmp/someFile-ex10-11.dat", wfContext,
        {"sampleId": sample_id}
    )
    if trigger_job is None:
        print("Failed to set data trigger job")
    else:
        trigger_job = lwfManager.wait(trigger_job.getJobId())
        print(f"\n* out11: Data trigger executed: {trigger_job}")


    # ****************************************************************************
    # dump everything we know about the workflow

    workflow_data = lwfManager.dumpWorkflow(wf.getWorkflowId())
    print("\n* out99: Workflow dump structure:" + json.dumps(workflow_data, indent=2, default=str))
    print("\n")


    # ****************************************************************************
