import multiprocessing as mp
import sys
import re
import couchdb
import os
import xml.etree.ElementTree as ET
BATCH_SIZE = 10000
SMALL_BATCH_SIZE = 5000

dbName = 'test'

def parseLink(link):
    return {
                '_id' : 'L_' + link.attrib['ID'],
                'ID1' : link.attrib['O1-ID'],
                'ID2' : link.attrib['O2-ID'],
           }

def sendLinks(LINKS, db):
    links = [parseLink(link) for link in LINKS]
    for i in range(len(links))[::BATCH_SIZE]:
        db.update(links[i:i+BATCH_SIZE])

def parseObject(obj):
    return {
               '_id' : 'O_' + obj.attrib['ID']
           }

def sendObjects(OBJECTS, db):
    objs = [parseObject(obj) for obj in OBJECTS]
    for i in range(len(objs))[::BATCH_SIZE]:
        db.update(objs[i:i+BATCH_SIZE])

def sendAttributes(ATTRIB, db):
    newKey = ATTRIB.attrib['NAME']
    itemType = ATTRIB.attrib['ITEM-TYPE']
    idDict = {itemType[0] + '_' + attr.attrib['ITEM-ID'] : 
              attr.find('COL-VALUE').text
              for attr in ATTRIB}
    idList = list(idDict.keys())
    for i in range(len(idList))[::SMALL_BATCH_SIZE]:
        batchDocs = db.view('_all_docs', 
                            keys=idList[i:i+SMALL_BATCH_SIZE],
                            include_docs=True)
        for row in list(batchDocs.rows):
            row['doc'][newKey] = idDict[row.id]
        db.update([row['doc'] for row in batchDocs.rows])

def parseFile(filename):
    s = couchdb.Server()
    try:
        db = s[dbName]
    except:
        print("Failure to connect! Aborting.")
    print("Parsing", filename)
    tree = ET.parse(filename)
    root = tree.getroot()

    if root.find('LINKS'):
        sendLinks(root.find('LINKS'), db)

    elif root.find('OBJECTS'):
        sendObjects(root.find('OBJECTS'), db)

    else:
        sendAttributes(root[0][0], db)

if __name__ == '__main__':
    server = couchdb.Server()
    try:
        server.create(dbName)
    except:
        try:
            s = server[dbName]
        except:
            print("Unable to connect to couchdb. Try starting it?")
            exit()

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
    elif '--just-objects' in sys.argv:
        filenames = [f for f in os.listdir('splitXML')
                     if re.match(r'^OBJECTS.*.xml$', f)]
    elif '--just-object-type' in sys.argv:
        filenames = [f for f in os.listdir('splitXML')
                     if re.match(r'^object-type.*.xml$', f)]
    elif '--one-file' in sys.argv:
        filenames = [sys.argv[-1]]

    elif '--just-views' in sys.argv:
        filenames = []
    
    else:
        filenames = sorted(os.listdir('splitXML'))

    objectFiles = ['splitXML/' + f for f in filenames if re.match(r'[A-Z].*xml$', f)]
    attrFiles = ['splitXML/' + f for f in filenames if re.match(r'[a-z].*xml$', f)]

    for f in objectFiles:
        while True:
            try:
                parseFile(f)
                break
            except:
                print("Failure when sending {0}. Retrying...".format(f))


    for f in attrFiles:
        while True:
            try:
                parseFile(f)
                break
            except:
                print("Failure when sending {0}. Retrying...".format(f))

    print("Creating views...")

    views = {'views': 
                {'count_objects': 
                    {'reduce': '_count', 
                     'map': "function (doc) {\n  if(doc['object-type'])" +
                     "{\n    emit(doc._id, 1);\n  }\n}"
                    }, 
                 'count_all': 
                     {'reduce': '_count', 
                      'map': 'function (doc) {\n  emit(doc._id, 1);\n}'
                     }, 
                 'count_links': 
                     {'reduce': '_count',
                      'map': "function (doc) {\n  if(doc['link-type'])" +
                      "{\n    emit(doc._id, 1);\n  }\n}"
                     }, 
                 'find_no_link_type': 
                     {'map': "function (doc) {" +
                      "\n  if(doc._id[0] == 'L' && !doc['link-type'])" +
                      "{\n    emit(doc._id, doc);\n  }\n}"
                     }
                }, 
             'language': 'javascript'
            }
    server[dbName]['_design/counter'] = views
