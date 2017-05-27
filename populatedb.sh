if [ ! -f dblp-data.xml.gz ]; then
    echo "Downloading xml gzip from "\
    "http://kdl.cs.umass.edu/databases/dblp-data.xml.gz..."
    wget http://kdl.cs.umass.edu/databases/dblp-data.xml.gz
fi

if [ ! -f dblp-data.xml ]; then
    echo "Unzipping xml gzip..."
    gunzip dblp-data.xml.gz
fi

echo "Splitting dblp-data into little chunks..."
python3 splitter.py

echo "Populating database with little chunks..."
python3 sendcouch.py
