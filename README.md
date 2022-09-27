# lwfm

"Local Workflow Manager"

An implementation of a 4-part framework for locally-managed inter-site workflow interoperability.

In a nutshell...

lwfm defines a site interface in four parts - Auth, Run, Repo, Spin, each with a limited set of verbs and a generous arbitrary
argument passing scheme.  A Site which implements these four pillars can then be plugged into a workflow which is written in terms
of those verbs.

A small number of local components track and orchestrate the cross-site workflows, permitting job chaining and the weaving of a
digital thread.

The work is based on a paper presented at the Smoky Mountains Conference in August 2022, and available (temporarily until official
publication) here: https://drive.google.com/file/d/1c3YEVmEAUjbI5urj4PiV2TtjzBUzLlws/view?usp=sharing

More detailed documentation about the implementation is presented in a Word doc in this git project.

