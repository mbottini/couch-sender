# couch-sender

Tool for splitting up and importing DBLP data into CouchDB. Testing on Mac and Windows is welcome.

## Dependencies:

* Install CouchDB.
  [Windows](http://docs.couchdb.org/en/2.0.0/install/windows.html)
[Mac](http://docs.couchdb.org/en/2.0.0/install/mac.html)
[Linux](http://docs.couchdb.org/en/2.0.0/install/unix.html)

* `wget` for the folks on Arch who don't know that they don't have a super-basic
  Unix tool.

* `gunzip` for unzipping the downloaded `.gz` file.

* `python3`, of course.

## Running the Easy Way:

* Download this repository with `git clone
  https://github.com/mbottini/couch-sender.git`.

* Run `sh populatedb.sh` and watch it go.

## Running Individual Components

* Run `splitter.py` to split the data into each category. The `.gz` file is
  currently hardcoded.

* Run `sendcouch.py` to send the chunks into couchdb

Total running time on my beefy desktop is about 22 minutes. Your mileage
may vary.



