# dwsearch

a data portal, primarily for use with darwin core data
allows for customizable searches of data indexed by elasticsearch and links to each records canonical representation in some kind of resolver

## how to run it

first of all, edit the config.yaml file:

* set up your elasticsearch indices and hostnames
* take a look at the form and core definitions
* change the `cookiekey`

(see the wiki for more information)

run dwsearch on port 8080: `./dwsearch.py`
