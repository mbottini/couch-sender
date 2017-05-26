import os
import xml.etree.ElementTree as ET

def parseAll():
    os.makedirs("splitXML", exist_ok=True)

    print("Inside root splitter.")
    tree = ET.parse('dblp-data.xml')
    root = tree.getroot()

    child = root.find('OBJECTS')
    print("Splitting OBJECTS")
    filename = 'splitXML/' + child.tag + ".xml"
    with open(filename, 'w') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<!DOCTYPE PROX3DB SYSTEM "prox3db.dtd">\n')
        f.write('<PROX3DB>\n')
        f.write(ET.tostring(child, encoding="unicode"))
        f.write('</PROX3DB>')

    child = root.find('LINKS')
    print("Splitting", child.tag)
    filename = 'splitXML/' + child.tag + ".xml"
    with open(filename, 'w') as f:
        f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write('<!DOCTYPE PROX3DB SYSTEM "prox3db.dtd">\n')
        f.write('<PROX3DB>\n')
        f.write(ET.tostring(child, encoding="unicode"))
        f.write('</PROX3DB>')

    child = root.find('ATTRIBUTES')
    for attribute in child:
        print("Splitting", attribute.attrib['NAME'])
        filename = 'splitXML/' + attribute.attrib['NAME'] + '.xml'
        with open(filename, 'w') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<!DOCTYPE PROX3DB SYSTEM "prox3db.dtd">\n')
            f.write('<PROX3DB>\n')
            f.write(ET.tostring(attribute, encoding="unicode"))
            f.write('</PROX3DB>')

parseAll()
