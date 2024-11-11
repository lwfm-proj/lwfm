# lwfm

lwfm: "Lightweight WorkFlow Manager"

A lightweight implementation of a 4-part framework for locally-managed inter-site workflow interoperability.

In a nutshell...

lwfm defines a site interface in four parts - Auth, Run, Repo, Spin, each with a limited set of verbs and a generous arbitrary
argument passing scheme.  A Site which implements these four pillars can then be plugged into a workflow which is written in terms of those verbs.  The framework is applicable to workflows: 1) in-situ, 2) intra-site, and 3) inter-site.

A local component tracks and orchestrates the cross-site workflows, permitting job chaining and the weaving of a
digital thread.  This is exposed to workflows via the LwfManager class, which permits setting workflow event handlers and persisting provenancial metadata.  In addition to this, the framework itself will persist additional tracking info which permits interrogation of both control and data flows - what jobs triggered what other jobs, what data elements were created and then used by which jobs.  Job status is normalized across Sites permitting a unified flow and view.  

The work is based on a paper presented by A. Gallo et al at the Smoky Mountains Conference in August 2022, and available
here: https://drive.google.com/file/d/1c3YEVmEAUjbI5urj4PiV2TtjzBUzLlws

Setup:
1. Get the python libs (see requirements.txt)
2. Run lwfm.sh to start the lwfm service
3. Write multi-site Python workflows in terms of lwfm verbs

You can then author your own Site drivers based on the examples provided.  

The initial implementation is the work of A. Gallo, D. Hughes, G. McBride with the sponsorship of GE Research.
