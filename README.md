# lwfm

lwfm: "Lightweight WorkFlow Manager"

A lightweight implementation of a 4-part framework for locally-managed inter-site workflow interoperability.

lwfm defines a site interface in four parts - Auth, Run, Repo, Spin, each with a limited set of verbs and a generous arbitrary argument passing scheme. A Site which implements these four pillars can then be plugged into a workflow which is written in terms of those verbs. The framework is applicable to workflows: 1) in-situ, 2) intra-site, and 3) inter-site.

There are many workflow tools, commercial and open source. What makes lwfm different (as a reference implementation) is that it provides an abstraction layer for interacting with remote sites, and with its middleware component it tracks the data used by the workflow. Running jobs on a remote site looks the same as running jobs locally. Each job has an identifier, parent-child relationships are tracked, and data creation and usage is tracked, tagged to the job id. 

Being lightweight, the framework is designed to be portable and used easily by an individual who needs to orchestrate workflows across diverse compute resources. It can be used on a desktop, or copied out to a remote site (e.g. an allocation at a remote lab or HPC facility) as a distinct instance, with its data later liberated back to a local instance if desired. It can be used within an HPC allocation to manage workflows across nodes in the allocation in-situ.

An easily deployable middleware component tracks and orchestrates the cross-site workflows, permitting job chaining and the weaving of a digital thread.  This is exposed to workflows via the LwfManager class, which permits setting workflow event handlers and persisting provenancial metadata. In addition to this, the framework itself will persist additional tracking info which permits interrogation of both control and data flows - what jobs triggered what other jobs, what data elements were created and then used by which jobs. Job status is normalized across Sites permitting a unified flow and view.  

The four Site pillars and their actions are:
    Auth: login, check login
    Run: run job, get job status, cancel job 
    Repo: put data w. metadata, get data by metadata
    Spin: list resources, create, destroy

The lwfManager middleware provides the following additional functions:
    - regularized logging linked to job id
    - create workflow, find jobs and data by workflow id
    - create metadata sheets for data objects, find data by metadata 
    - register job and data event handlers 
    - emit job and data events
    - poll remote Sites for job status and broadcast to any listening event handlers

Data events are keyed on metadata. Management of the data itself is Site specific and might be in a filesystem, or in some other Site-managed repository. Metadata is managed by lwfm, and since the metadata sheet links to the URL of the managed data element, the lwfm metadata is the index into the user's data locally and on remote sites.

The work is based on a paper presented by A. Gallo et al at the Smoky Mountains Conference in August 2022, and available in this git project along with other source material. That work itself was informed by an in-house project at GE Research to develop the workflow tooling which covers the "type 1" in-situ and "type 2" enterprise workflows also enabled by lwfm, while lwfm adds cross-site "type 3" workflow capability. The contributions of D. Hughes and G. McBride are recognized.

Setup:
1. Get the project from github
2. Run lwfm.sh to start the lwfm service (or call it directly from Python)
3. Write multi-site Python workflows in terms of lwfm verbs.

You can then author your own Site drivers based on the examples provided.  


