# couch-sender

Tool for splitting up and importing DBLP data into CouchDB. Testing on Mac and Windows is welcome.

## Config

* Install CouchDB. [Windows](http://docs.couchdb.org/en/2.0.0/install/windows.html)
[Mac](http://docs.couchdb.org/en/2.0.0/install/mac.html)
[Linux](http://docs.couchdb.org/en/2.0.0/install/unix.html)

* Download this repository with `git clone https://github.com/mbottini/couch-sender.git`.

* Download the DBLP data into that directory from http://kdl.cs.umass.edu/databases/dblp-data.xml.gz.

* Unzip the DBLP data with `gunzip dblp-data.xml.gz`.

## Running

* Run `splitter.py` to split the data into each category.

* Run `sendcouch.py` on both `LINKS.xml` and `OBJECTS.xml`. Because these documents create the IDs,
they have to be run first. For now, I do `ls | grep ^[A-Z]+.xml | python3 sendcouch.py`.

* Run `sendcouch.py` on the remaining documents. I do the opposite of the above `ls` command,
doing `ls | grep ^[a-z].*.xml | python3 sendcouch.py`.

Total running time on my beefy desktop is about 2 hours. Multiprocessing and some less
stupid decisions should bring it up some.



