import multiprocessing as mp
import sys
import re
import couchdb
import os
import xml.etree.ElementTree as ET
BATCH_SIZE = 10000
SMALL_BATCH_SIZE = 5000

def parseLink(link):
    return {
                '_id' : 'L_' + link.attrib['ID'],
                'ID1' : link.attrib['O1-ID'],
                'ID2' : link.attrib['O2-ID'],
           }

def sendLinks(LINKS):
    links = [parseLink(link) for link in LINKS]
    for i in range(len(links))[::BATCH_SIZE]:
        lock.acquire()
        try:
            db.update(links[i:i+BATCH_SIZE])
        finally:
            lock.release()

def parseObject(obj):
    return {
               '_id' : 'O_' + obj.attrib['ID']
           }

def sendObjects(OBJECTS):
    objs = [parseObject(obj) for obj in OBJECTS]
    for i in range(len(objs))[::BATCH_SIZE]:
        lock.acquire()
        try:
            db.update(objs[i:i+BATCH_SIZE])
        finally:
            lock.release()

def sendAttributes(ATTRIB):
    newKey = ATTRIB.attrib['NAME']
    itemType = ATTRIB.attrib['ITEM-TYPE']
    idDict = {itemType[0] + '_' + attr.attrib['ITEM-ID'] : 
              attr.find('COL-VALUE').text
              for attr in ATTRIB}
    idList = list(idDict.keys())
    for i in range(len(idList))[::SMALL_BATCH_SIZE]:
        lock.acquire()
        try:
            batchDocs = db.view('_all_docs', 
                                keys=idList[i:i+SMALL_BATCH_SIZE],
                                include_docs=True)
            for row in list(batchDocs.rows):
                row['doc'][newKey] = idDict[row.id]
            db.update([row['doc'] for row in batchDocs.rows])
            #docsList = [row['doc'] for row in batchDocs.rows]
            #for doc in docsList:
            #    doc[newKey] = idDict[doc['_id']]
        finally:
            lock.release()

def parseFile(filename):
    print("Parsing", filename)
    tree = ET.parse(filename)
    root = tree.getroot()

    if root.find('LINKS'):
        sendLinks(root.find('LINKS'))

    elif root.find('OBJECTS'):
        sendObjects(root.find('OBJECTS'))

    else:
        sendAttributes(root[0][0])

def init(l, database):
    global lock
    global db
    lock = l
    db = database

if __name__ == '__main__':
    if '-d' in sys.argv:
        filenames = [f for f in os.listdir('splitXML') 
                     if re.match(r'title7', f)]
    elif '-o' in sys.argv:
        filenames = [f for f in os.listdir('splitXML')
                     if re.match(r'^[A-Z].*.xml$', f)]
    elif '-a' in sys.argv:
        filenames = [f for f in os.listdir('splitXML')
                     if re.match(r'^[a-z].*.xml$', f)]
    elif '-l' in sys.argv:
        filenames = ['link-type1.xml']
    else:
        filenames = os.listdir('splitXML').sort()
    server = couchdb.Server()
    try:
        database = server.create('test')
    except:
        database = server['test']
    l = mp.Lock()
    num_workers = mp.cpu_count()
    pool = mp.Pool(initializer=init, initargs=(l, database), processes=num_workers)


    objectFiles = ['splitXML/' + f for f in filenames if re.match(r'[A-Z].*.xml$', f)]
    attrFiles = ['splitXML/' + f for f in filenames if re.match(r'[a-z].*.xml$', f)]

    procList = []

    pool.map(parseFile, objectFiles)
    pool.map(parseFile, attrFiles)

  #for i in range(len(objectFiles))[::2]:
  #    p1 = Process(target=parseFile, args=(objectFiles[i],))
  #    p1.start()
  #    if i + 1 < len(objectFiles):
  #        p2 = Process(target=parseFile, args=(objectFiles[i+1],))
  #        p2.start()
  #    p1.join()
  #    if i + 1 < len(objectFiles):
  #        p2.join()
