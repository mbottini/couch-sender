import multiprocessing as mp
import sys
import re
import couchdb
import os
import xml.etree.ElementTree as ET
BATCH_SIZE = 10000
SMALL_BATCH_SIZE = 1000

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

    idDict = {}
    for attrValue in ATTRIB:
        itemID = itemType + "_" + attrValue.attrib['ITEM-ID']
        idDict[itemID] = attrValue.find('COL-VALUE').text
        if len(idDict) == SMALL_BATCH_SIZE:
            lock.acquire()
            try:
                batchDocs = db.view('_all_docs', 
                                keys=list(idDict.keys()),
                                include_docs=True)
                for doc in list(batchDocs.rows):
                    doc[newKey] = idDict[doc['id']]
                db.update(batchDocs.rows)
            finally:
                lock.release()

    if idDict:
        lock.acquire()
        try:
            batchDocs = db.view('_all_docs', 
                            keys=list(idDict.keys()),
                            include_docs=True)
            for doc in list(batchDocs.rows):
                doc[newKey] = idDict[doc['id']]
            db.update(batchDocs.rows)
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
    filenames = os.listdir('splitXML')
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
