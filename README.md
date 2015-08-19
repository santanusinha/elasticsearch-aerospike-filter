# Background
As nice as ES is, index refreshes are a pain. However, the features it provides far outweigh this (minor in most cases) drawback.

However, there are unavoidable real-time scenarios where we need to check values in some transactional DB like redis or aerospike etc to filter out unwanted documents. The transactional system gets updated faster or at a different rate than ES refresh. 

This sample project uses the native script functionality to inject a filter that queries aerospike to determine whether to accept a result or not.

## Pre-requisites

 - Use the node client to connect to elasticsearch (This is anyways recommended).
 - Keep the Aerospike connection in a singleton
 - Make sure that the instantiation of the singleton happens before the ES connection is establised (long live dependency injection!!)
## Caveats

- Since this is a script filter it gets documents one by one. Will see if we can optimize this somehow.
- The script is not called for every doc, only the one that passes the other filters. ES does the optimization to filter out stuff before it makes the call to script filters. (We should doublecheck this probably, local tests suggest this to be true)

NOTE: This same technique can be used in other places where scripts are allowed, for example post filters and scorers. Will try to add some examples of these things later.
