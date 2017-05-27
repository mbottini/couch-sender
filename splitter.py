import os
import re

attrRE = re.compile(r'^.*NAME="(.*)".*$')
BATCH_SIZE = 10000

def createDocFiles(filename, docType):
    print("Splitting", docType)
    prelude = ('<?xml version="1.0" encoding="UTF-8"?>\n' +
              '<!DOCTYPE PROX3DB SYSTEM "prox3db.dtd">\n' +
              '<PROX3DB>\n' +
              '    <{0}>\n'.format(docType))
    epilogue = '    </{0}>\n</PROX3DB>'.format(docType)
    with open(filename) as f:
        fileNumber = 1
        counter = 0

        # Get to the OBJECTS portion of the file.
        currentLine = f.readline()
        while currentLine != '    <{0}>\n'.format(docType):
            currentLine = f.readline()
        currentLine = f.readline()

        #Now, we write BATCH_SIZE objects to each file.

        w = open('splitXML/' + docType + str(fileNumber) + '.xml', 'w')
        w.write(prelude)
        currentFile = ""
        while currentLine != '    </{0}>\n'.format(docType):
            if counter == BATCH_SIZE:
                w.write(currentFile)
                w.write(epilogue)
                w.close()
                fileNumber += 1
                w = open('splitXML/' + docType + str(fileNumber) + '.xml', 'w')
                w.write(prelude)
                currentFile = ""
                counter = 0
            currentFile += currentLine
            currentLine = f.readline()
            counter += 1
        w.write(currentFile)
        w.write(epilogue)
        w.close()

def createAttributeFiles(filename):
    prelude = ('<?xml version="1.0" encoding="UTF-8"?>\n' +
              '<!DOCTYPE PROX3DB SYSTEM "prox3db.dtd">\n' +
              '<PROX3DB>\n' +
              '    <ATTRIBUTES>\n')
    epilogue = '        </ATTRIBUTE>\n    </ATTRIBUTES>\n</PROX3DB>'

    with open(filename) as f:
        fileNumber = 1
        counter = 0

        # Get to the ATTRIBUTES portion of the file.
        currentLine = f.readline()
        while currentLine != '    <ATTRIBUTES>\n':
            currentLine = f.readline()
        currentLine = f.readline()

        fileNumber = 1
        while currentLine and currentLine != '    </ATTRIBUTES>\n':
            attrName = re.match(r'^.*NAME=\"(.*?)\".*$', currentLine).group(1)
            print("Splitting", attrName)
            newPrelude = prelude + currentLine
            currentLine = f.readline()
            counter = 0
            fileNumber = 1
            w = open('splitXML/' + attrName + str(fileNumber) + '.xml', 'w')
            w.write(newPrelude)
            currentFile = ""
            while currentLine and currentLine != '        </ATTRIBUTE>\n':
                currentFile += currentLine
                currentLine = f.readline()
                currentFile += currentLine
                currentLine = f.readline()
                currentFile += currentLine
                currentLine = f.readline()
                counter += 1
                if counter == BATCH_SIZE:
                    w.write(currentFile)
                    w.write(epilogue)
                    w.close()
                    fileNumber += 1
                    w = open('splitXML/' + attrName + str(fileNumber) + '.xml', 'w')
                    w.write(newPrelude)
                    currentFile = ""
                    counter = 0
            w.write(currentFile)
            w.write(epilogue)
            w.close()
            currentLine = f.readline()

if __name__ == '__main__':
    os.makedirs('splitXML', exist_ok=True)
    createDocFiles('dblp-data.xml', 'OBJECTS')
    createDocFiles('dblp-data.xml', 'LINKS')
    createAttributeFiles('dblp-data.xml')

