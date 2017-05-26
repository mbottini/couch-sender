import couchdb
import sys
import xml.etree.ElementTree as ET
import threading

BATCH_SIZE = 10000

def parseLink(link):
    return {
                '_id' : 'L_' + link.attrib['ID'],
                'ID1' : link.attrib['O1-ID'],
                'ID2' : link.attrib['O2-ID'],
           }

def sendLinks(LINKS):
    links = [parseLink(link) for link in LINKS]
    for i in range(len(links))[::BATCH_SIZE]:
        print("At", i)
        db.update(links[i:i+BATCH_SIZE])

def parseObject(obj):
    return {
               '_id' : 'O_' + obj.attrib['ID']
           }

def sendObjects(OBJECTS):
    objs = [parseObject(obj) for obj in OBJECTS]
    for i in range(len(objs))[::BATCH_SIZE]:
        print("At", i)
        db.update(objs[i:i+BATCH_SIZE])


def sendAttributes(ATTRIB):
    newKey = ATTRIB.attrib['NAME']
    itemType = ATTRIB.attrib['ITEM-TYPE']

    docList = []
    counter = 1

    for attrValue in ATTRIB:
        itemID = itemType + "_" + attrValue.attrib['ITEM-ID']
        newValue = attrValue.find('COL-VALUE').text

        doc = db[itemID]
        doc[newKey] = newValue
        docList.append(doc)
        if len(docList) == BATCH_SIZE:
            print("On", counter * BATCH_SIZE)
            db.update(docList)
            docList = []
            counter += 1



def parseFile(filename):
    print("Parsing", filename)
    tree = ET.parse(filename)
    root = tree.getroot()

    if root.find('LINKS'):
        sendLinks(root.find('LINKS'))

    elif root.find('OBJECTS'):
        sendObjects(root.find('OBJECTS'))

    else:
        sendAttributes(root[0])

if __name__ == '__main__':
    server = couchdb.Server()
    try:
        db = server.create('test')
    except:
        db = server['test']

    for filename in sys.argv[1:]:
        parseFile(filename)

    


