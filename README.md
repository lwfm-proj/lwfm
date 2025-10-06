# lwfm

lwfm: lightweight workflow manager

the "yet another workflow tool the world doesn't need"


### quick start





### history

* ~2018: a project to orchestrate long-running workflows is 








propose training surrogate models from HPC data; Gallo writes Python prototype; contract dev team builds "DT4D" - Digital Thread for Design, still in production; allows authoring of workflows across HPC and non-HPC computes, tracking all data usage and job interdependencies, support long running workflows, on-prem & cloud; patented, presented at Sandia; Gallo begins communicating on workflow topics with national lab staff, participates in workshops, etc.

    - 2021: capability to drive in-situ workflows developed for steering of HPC apps (e.g. Genesis CFD) within the allocation from outside (e.g. from DT4D)

    - 2022: NERSC releases Superfacility API to permit external control of HPC jobs

    - 2023: Gallo presents 3-type workflow model & 4-pillar site architecture at NAFEMS & lwfm reference implementation

    - 2025: lwfm revisited for use in quantum computing applications


why yet another workflow tool?
    - workflow tooling landscape is full of similar-to tools, many liked by specific scientific or business domains
    - many do not address interoperability
    - most do not address data provenance using FAIR principles


3 workflow types:
    - 1: in-situ (e.g. Genesis)
    - 2: enterprise (e.g. DT4D)
    - 3: intra-enterprise (e.g. lwfm) <-- lwfm can be used for all 3 types

    <figure>


4 pillars of an interoperable site: <-- a very limited set of verbs --> LLM potential
    - auth: login
    - run: execute job, check status, cancel
    - repo: put, get, notate, find
    - spin: list resources, (de)provision

    <figure>
    a site implementation is ~200-300 lines of Python code, with reuse via object inheritance (e.g. extension of a "LocalSite" driver) 
        - LocalSite - run anything local as a first class "job" with tracking
        - LocalSiteVenv - local with virtualenv / sandboxing support
        - IBMQuantumSite - run jobs on IBM quantum cloud, IBM simulators 
        - (DT4D)
        - (AWS)


interoperability goals:
    - normalize site differences into a common set of verbs
    - write workflows which run on any site with a similar compute architecture (i.e. this is *not* a portability framework, it is a tool for interoperability)
    - track all job dependencies and data usage no matter where run ("digital thread") to aid reproducibility, data reuse, data provenance
    - insulate (via the site abstraction) 3rd party dependencies (e.g. different qiskit library versions for different quantum vendors); circuits are portable between these site sandboxes via industry-standard QASM circuit format


Example use cases:
    1. run circuit construction, then target transpilation and execution on a set of quantum backends for benchmarking, then post-process results
    2. same as above, but handling the case where various backends have their own 3rd party dependencies (e.g. different qiskit versions) by running each backend in its own site sandbox
    3. populate data sets by running batches of circuits, feed results (data & metadata) to classical ML models, assess the results
    4. run a set of workflows / experiments on a remote allocation (e.g. ORNL) capturing the run & data flow digital threads, then haul back all the results to a local system for analysis / workflows related back to the original experiments
    5. in-situ workflows for iterative / variational quantum algorithms


Future work:
    - fix data / metadata import / export (use case #4 above)
    - reinstate / author site drivers as needed
    - other misc. bugs


Examples:



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

