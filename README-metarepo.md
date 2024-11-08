# MetaRepo
MetaRepo is a library that aids in the storage and retrieval of metadata files called *metasheets*. MetaRepo helps to strike a balance between customization (storing any arbitrary metadata) and organization (making sure that metadata is meaningful). Metasheets have specific fields that must always be filled in, and belongs to a *site* and a *target* class that may require certain other user defined fields. For example, a metasheet might correspond to a file stored on disk, and a file target class might be created that requires a file size, location on disk, file name, and so forth.

## Metasheet Format
Each metasheet has the following pieces of data:

 - docId: a UUID providing a unique identifier for each metasheet
 - docSetId: a list of of UUIDs, allowing multiple metasheets to be searched and retrieved together. This list may be empty.
 - status: an integer enum. 0 is "IN_PROGRESS", 1 is "AVAILABLE", and 2 is "DELETED". Only AVAILABLE files are retrieved in a typical search. User defined subclasses may add other status options.
 - displayName: a human readable name that can be used in user interfaces
 - timestamp: the time the file was initially created (even if it's since been updated)
 - userMetadata: a set of key-value pairs. This is entirely user controlled, and arbitrary for each metasheet.
 - targetClass: a string identifying the target, the type of object the metadata corresponds to. There must be a corresponding module in the src/MetaTargets directory.
 - targetMetadata: a set of key-value pairs. This undergoes validation based on targetClass.
 - siteClass: a string identifying the site, the location of the object the metadata corresponds to. There must be a corresponding module in the src/MetaSites directory.
 - siteMetadata: a set of key-value pairs. This undergoes validation based on siteClass.
 - frameworkArchive: A list showing how the metasheet has changed over time. Any time a framework level parameter is updated (docSetId, status, displayName), the archive stores the previous value, the timestamp of the update, the user performing the update, and an optional comment.
 - metadataArchive: An archive similar to the frameworkArchive, corresponding to changes in userMetadata.
 -  targetMetadataArchive: An archive similar to the frameworkArchive, corresponding to changes in targetMetadata.
 -  siteMetadataArchive: An archive similar to the frameworkArchive, corresponding to changes in siteMetadata.

## Config File
MetaRepo requires a config file called "MetaRepo/metarepo.conf" to be used. It's in the ini file format, with a series of config headers and parameters. To understand how headers and parameters are used, see the example below. The current config headers and parameters are:

- BASE
  - repotype: Which type of repo to use to store the metasheets (required)
- AUTHSERVICE
  - admin_url: The base URL for the auth service (see the auth section below) (required)
  - checkAuth_endpoint: The API endpoint to check user authentication (see the auth section below) (required)
- ELASTICSEARCH
  - elastic_user: If the elasticsearch repo is used, this is the username for database queries
  - elastic_password: If the elasticsearch repo is used, this is the password for database queries
  - elastic_url: If the elasticsearch repo is used, this is the URL of the database
  - cert_fingerprint: If the elasticsearch repo is used, this is the cert fingerprint of the database
- LOCAL
  - local_file: If the local repo is used, this is the file to store data in, relative to the run directory. Defaults to "meta.repo"
 - SQL
    - db_filename: If the SQL repo is used, this is the filename of the SQL database

### Example
This is an example of a complete, functional config file using the elasticsearch repo type.

    [BASE]
    repotype = SQLRepository
    
    [AUTHSERVICE]
    admin_url = https://metarepo.ge.com/
    checkAuth_endpoint = auth/checkAuthorization
    
    [ELASTICSEARCH]
    elastic_url = https://localhost:9200
    elastic_user = elastic
    elastic_password = myPassword
    cert_fingerprint = abcde123456790abcde1234567890


## Running MetaRepo

To run MetaRepo, a config file must be created and stored in the base MetaRepo/ directory. MetaRepo itself is a FastAPI app, and users may wish to view [the FastAPI documentation](https://fastapi.tiangolo.com) for more details on use. Alternatively, there are a couple easy ways to get started, depending on whether the user wishes to run MetaRepo directly on their machine, or use a docker container.

### Running MetaRepo Directly
MetaRepo requires Python 3.9 at a minimum. If you don't already have it, install from [the Python release site](https://www.python.org/downloads). First, make sure you have all dependencies installed. Assuming you're in the MetaRepo directory, you can install dependencies with this command:

    pip install --no-cache-dir --upgrade -r requirements.txt
Once you have your dependencies, you can start MetaRepo using uvicorn (automatically installed in the previous step):

    uvicorn src.metarepo:app --port 8000
Uvicorn has many optional parameters, [so the documentation may be of use](https://www.uvicorn.org). The user may update the port number to fit their needs.

### Running MetaRepo Through Docker
MetaRepo includes a Dockerfile, allowing easy deployment. It's recommended to use the sql repo type, unless the user has their own custom repo. After creating the metarepo.conf file, simply use the following commands in the MetaRepo/ directory:

    docker build -t metarepo .
    docker run -p 8000:8000 metarepo

Make sure to include the trailing period in the "docker build" command! This pair of commands will create a docker container and then run a docker image. If the user wants to use a port other than 8000, the docker run command needs to be updated. For example, to run on port 80, the -p field should be changed to "-p 80:8000". Note that there are many ways to use docker, and this guide is meant specifically as a quick start.

# API

MetaRepo includes four API endpoints for storing and retrieving metadata. Each endpoint has a set of parameters that must be provided via JSON body. Authentication is provided by a bearer token, so the user must also supply an "Authorization: Bearer \<token\>" header field.

## POST /notate

### Parameters
- docId (string)
- docSetId (list of strings)
- displayName (string)
- userMetadata (object--key value pairs must be strings, booleans, or numbers)
- siteClass (string)
- siteMetadata (object--key value pairs must be strings, booleans, or numbers)
- targetClass (string)
- targetMetadata (object--key value pairs must be strings, booleans, or numbers)
- archiveComment (string)

### Return Type
A JSON object consisting solely of the key "docId" with a value corresponding to the metasheet's docId.

### Description
Notate allows us to store or update metasheets. If a docId is not included, a new metasheet is created. In this case, the siteClass and targetClass are required and must identify a module in the src/MetaSites/ and src/MetaTargets/ directories respectively. The siteMetadata and targetMetadata will then undergo validation based on the site and target classes. archiveComment will be ignored. All other fields are optional and will default to an empty object of the appropriate type if not included.

If a docId is included, it will update an existing metasheet. If a document with the provided docId is not found in the repo, an error will be returned. archiveComment is optional and used to identify why the change is being made. All other fields are optional, and will update the document if provided.

## GET /find

### Parameters
- filters (object--key value pairs must be strings, booleans, or numbers)

### Return Type
A list of metasheets matching the filters.

### Description 
The filter is a set of key value pairs, each filtering the metasheets. All filters must pass for a metashet to be returned. To create a filter for base, framework level parameters (like docId), we can simply include the name of the parameter as the filter key. To create a filter for metadata parameters, use a period to separate the metadata type and the parameter name. For example, to search for a particular docId, we can use this request body:

    {"filters" : {"docId" : "43385093-f6c3-4969-8040-9b75d110b84c"}}

If we want to search for all files with the DT4D target type and a user metadata "foo"="bar", we could instead use this body:

    {"filters" : {"targetClass" : "DT4DTarget", "userMetadata.foo" : "bar"}}

Only documents with an AVAILABLE status will be returned. The maximum number of results returned will depend on the repository.

## POST admin/forceNotate

### Parameters
Arbitrary json

### Return Type
A JSON object consisting solely of the key "docId" with a value corresponding to the metasheet's docId.

### Description
An admin only endpoint. This will add an arbitrary, unvalidated metasheet. If no docId is included, one will be generated and added; otherwise, no changes are made. If a docId is included and matches an already existing metasheet, that metasheet will be updated to match the new json, otherwise a new metasheet will be created. Because this metasheet is unvalidated, it does not necessarily have typical fields like status and targetSite. 

## GET admin/find_all

### Parameters
- page (int)

### Return Type
A list of metasheets matching the filters.

### Description
An admin only endpoint. It is identical to the /find endpoint, except no filters are required and all documents are returned regardless of status. Because a repo might be limited in the number of results returned, the "page" parameter allows multiple requests. If a repo returns only 500 results at a time, for example, setting page=1 will return results 501-1000, page=2 will return 1001-1500, and so on.

# Authentication
The auth.py module provides a base authentication API. The included sample auth.py may be overwritten by the user if they wish to include their own security scheme. The sample requires an external API (with URL stored in the config) that takes in a Bearer token and returns a JSON similar to the following:

    {"ownerGroups" : [{"idmGroupId" : "0000"}, {"idmGroupId" : "0005"}]},
     "username" : "DavidHughes",
     "expiresAt" : 0}

"ownerGroups" must have a list of objects with an "idmGroupId", each being a unique string. Note that groups are not directly used by MetaRepo, but are useful for custom modules. For example, the Elasticsearch Repo type includes a group with each metasheet; if a user does not belong to the group, they may not see it, providing extra security in a large organization with many departments. The "username" may also be used by submodules, and is provided in metasheet archives to show who has created or edited a file. Finally, "expiresAt" is a timestamp (in seconds since the Unix epoch) showing when a particular login expires.

# Repo Types
The method used for storing metasheets (for example, Elasticsearch or SQLite) is known as the repository, or repo. Users may create their own repo by adding a module to src/Repositories/, and inheriting from RepositoryBase. Three methods must be instantiated--find, notate, and update. Once a custom site is created, it may be used by setting its module name in the "BASE.repotype" field of the config file.

**find(self, filters: dict=None, groups: list=None, page: int=0) -> list[dict]**

filters: a set of filters to apply to the search.  find() should return all documents that match each key-value pair. If a period is in the key (for example, "targetMetadata.fileSize", it indicates that the first part of the key is a metadata type and the second part of the key is a subfield within that metadata.
groups: If included, the metasheet must belong to one of the included groups. Implementation of group membership is optional, and whether groups even exist depends on the user's auth class.
page: If this implementation returns a fixed maximum number of results, "page" creates an offset equal to page*max_results. For example, if the repo provides 500 results per find, setting page=1 should return results 501-1000.

return value: find() should return a list of metasheets, as described in the format section above, which fits all provided filters.

**notate(self, doc: dict) -> None**

doc: A metasheet to be added to the database, as described in the format section above.

return value: No return value is needed. However, in the event of errors, exceptions should be raised.

**update(self, doc_id: str, update_fields: dict) -> None**

doc_id: The document to be updated. It must already exist in the database.
update_fields: Key-value pairs to be updated. This is essentially a truncated metasheet, where every field is optional. If a field is not included, update() assumes it will be unchanged.

return value: No return value is needed. However, in the event of errors, exceptions should be raised.

# Target and Site Classes

Every metasheet belongs to a particular *target* and *site* that allow for custom validation. The target is the type of data the metasheet represents. For example, it might be a file on disk, or a a job in a workflow. The site is special information about where the metasheet is stored, allowing it to fit in larger systems. For example, it might be part of the DT4D digital threading application. To create a custom target or site, a module should be added to src/MetaTargets/ or src/MetaSites/ and inherit from MetaTargetBase or MetaSiteBase respectively. Note that for now, MetaTargetBase and MetaSiteBase are identical classes, and this is unlikely to change. Two methods must be instantiated--validate_metadata and updata_metadata.

**validate_metadata(self, doc : dict, user_info : dict) -> dict **

doc: An entire metasheet, as described in the format section above.
user_info: A user object, as described in the auth section above.

return value: A validated metadata field. Note that this ONLY returns the targetMetadata or siteMetadata field of a metasheet, not the entire metasheet.
