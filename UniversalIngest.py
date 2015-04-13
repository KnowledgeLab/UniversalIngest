#!/usr/bin/python

''' UNIVERSAL INGESTER
    Jan 3, 2014 - WCC - created.  Successfully tested by ingesting arXiv and some of pubmed.
    Jan 30, 2014 - WCC - added -stats option for previewing / gathering statistics on corpus
                to aid in the creation of the control file,
                DOC_ID based on AUTO_INCREMENT, REPLACE command, -append option.
'''
import sys, os, re, glob, MySQLdb, subprocess, csv
import argparse
import datetime
import getpass  # for getting password from user input
from lxml import etree      # import lxml
import json
import dicttoxml
import weakref
import StringIO
from bs4 import BeautifulSoup

##############################################################################################
# Function for uniformly converting from unicode to ASCII
##############################################################################################
def unicodeToString(s):
    try:
        result = s.encode('ascii', 'xmlcharrefreplace')
    except:
        result = ' '
    return result


###############################################################################
# Extract a string value from the text of a tree element - use the appropriate function that depends on the columnFlags
def GetValueFromElement_Text(elem):
    # get the text
    return elem.text

def GetValueFromElement_NoChildren(elem):
    # get the text, without showing the children
    s = []
    if elem.text is not None:
        s.append(elem.text)
    for child in elem:
        if child.tail is not None:
            s.append(child.tail)
    return ''.join(s)

def GetValueFromElement_Recurse(elem):
    # get the raw text, without processing the children within this node
    return etree.tostring(elem)

def RecurseNoTags(s, elem):
    # get the text, without showing the children
    if elem.text is not None:
        s.append(elem.text)
    for child in elem:
        RecurseNoTags(s, child)
        if child.tail is not None:
            s.append(child.tail)
    return s

def GetValueFromElement_RecurseNoTags(elem):
    # get the text, without showing the children
    s = []
    RecurseNoTags(s, elem)
    return ' '.join(s)

def GetValueFromElement_Identity(elem):
    # the element is really the value
    return elem

def CleanValue(val):
    valOut = 'NULL'
    if val is not None:
        val = val.strip()   # try this for now for stripping white space off either side of text
        val = val.encode("utf-8")
            # quotes don't hurt INT assignments - so simply always use them!
        val = '"' + MySQLdb.escape_string(val) + '"'
        #print '   escaped val = ' + val
        valOut = val
    return valOut

def GetValueFromElement(elem, func):
    if elem is not None:
        val = func(elem)
    return CleanValue(val)

########################################################################
def GetDocElems(tree, docTag):
    if docTag is None:
        docElems = [tree.getroot()]     # if no <docTag> is specified, use the root node
    else:
        docElems = tree.findall(docTag)
    return docElems

#---------------------------------------------------------------------------------------------
class CTableColumn:
    def __init__(self, name = '', type = '', dbFlags = '', tag = '', attrib = None, getValueFunction = GetValueFromElement_Text):
        self.name = name                # string name of the column in the database
        self.type = type                # string type field in MySQL language
        self.dbFlags = dbFlags          # string dbFlags to be used in MySQL when creating the table
        self.tag = tag                  # tag in XML source files that will map to this column
        self.attrib = attrib            # attribute that holds the value
        self.tagLower = tag.lower()     # lower case version of tag
        self.getValueFunction = getValueFunction

    def __repr__(self):
        return '[CTableColumn: name = %s, type = %s, dbFlags = %s, tag = %s, attrib = %s]' % (self.name, self.type, self.dbFlags, self.tag, self.attrib)

#---------------------------------------------------------------------------------------------
class CTable:
    def __init__(self, name='', forEachTag = None):
        self.name = name                # string name of the table
        self.columns = []               # list of CTableColumn's
        self.forEachTag = forEachTag    # when present, indicates that the there could be multiple entries in this table per document, each with the specified tag as a parent
        if forEachTag is not None:
            print '######################\n' * 4 + 'table ' + self.name + ' has forEachTag = ' + forEachTag

    def __repr__(self):
        return '[CTable: name = ' + self.name + ', \n\tcolumns: ' + `self.columns` + ']'

    def ColumnAdd(self, column):
        print '---------ColumnAdd(): column = ' + column.name
        self.columns.append(column)

#---------------------------------------------------------------------------------------------
class CControl:
    def __init__(self, sources = None, dbName = ''):
        if sources == None:
            sources = []

        self.sources = sources                      # list of (filename glob, beautifulSoup) pairs
        self.dbName = dbName                        # string - the name of the database to ingest
        self.docStartTag = ''                       # string - the name of the tag that marks the start of each document
        self.docIdTable = None                      # name of table  that holds the AUTO_INCREMENT value of DOC_ID
        self.tables = []                            # list of CTable's
        self.defines = []                           # list of definitions for text substitution of each subsequent line
        self.beautifulSoup = None                   # current value of beautifulSoup that is applied to files
        self.bJson = False                          # current value of bJson that is applied to files
        self.bFakeRoot = False                      # current value of bFakeRoot that is applied to files

    def __repr__(self):
        return '[CControl: dbName = ' + self.dbName + ', \n\sources: ' + `self.sources` + ',\n  tables = ' + `self.tables` + ']'

    def SourceFilenameAdd(self, filename):
        self.sources.append((filename, self.beautifulSoup, self.bJson, self.bFakeRoot, 'glob'))

    def SourceFileListAdd(self, listFilename):
        self.sources.append((listFilename, self.beautifulSoup, self.bJson, self.bFakeRoot, 'fileList'))

    def SourceFileIter_Init(self, source):
        if source[4] == 'glob':
            iFilename = glob.iglob(source[0])
            # shellCmd('ls ' + myGlob)
        else: # source[4] = 'fileList'
            print 'opening fileList: ', source[0]
            with open(source[0]) as f:
                iFilename = f.read().split()
                print "iFilename (", len(iFilename), ") ="#, iFilename
        return iFilename

    def SourceFileIter_Exit(self, source, iFilename):
        if source[4] == 'fileList':
            pass #iFilename.close()


    def TableAdd(self, name, forEachTag = None):
        self.tables.append(CTable(name, forEachTag))

    def TableGetLast(self):
        print '---------TableGetLast: ' + self.tables[-1].name
        return self.tables[-1]

    def BeautifulSoupSet(self, value):
        print 'CControl::BeautifulSoupSet() = '
        print value
        self.beautifulSoup = value

    def JsonSet(self, value):
        print 'CControl::JsonSet() = '
        print value
        self.bJson = value

    def FakeRootSet(self, value):
        print 'CControl::FakeRootSet() = '
        print value
        self.bFakeRoot = value
#---------------------------------------------------------------------------------------------
class CStat:
    def __init__(self):
        self.totalCount = 0                         # total count of occurences throughout corpora
        self.curCount = 0                           # current count of number of occurrences within current document
        self.minCount = 987654321                   # min number of occurrences within document
        self.maxCount = 0                           # max number of occurrences within document
        self.maxTextLength = 0                      # max number of characters that the text value requires

#---------------------------------------------------------------------------------------------
class CStats:
    def __init__(self):
        self.d = {}                                 # Dictionary of statistics, keyed by node path
        self.tags = []                              # list of tags at each level given current state of traversal
        self.tagLevels = 0                          # number of levels filled in, or next level to fill
        self.tagLevelsAllocated = 0                 # number of levels allocated

    def __repr__(self):
        return ''  #[CControl: dbName = ' + self.dbName + ', \n\tsources: ' + `self.sources` + ',\n  tables = ' + `self.tables` + ']'

    def UpdateDocStart(self):
        for dd in self.d.values():
            dd.curCount = 0

    def UpdateDocEnd(self):
        for dd in self.d.values():
            dd.totalCount += dd.curCount
            dd.maxCount = max(dd.maxCount, dd.curCount)
            dd.minCount = min(dd.minCount, dd.curCount)

    def Update(self, tree, control, iDoc):
        levelMin = 1    # minimum level for showing attributes, used to prevent useless and very long attributes at level 1
        if False:
            TreePrintPaths(tree)
        else:
            docElems = GetDocElems(tree, control.docStartTag)   # iterate over each document
            print '................... docElems (%d) =' % len(docElems)
            #print docElems
            for docElem in docElems:    # subtree corresponding to a document
                #print "docElem = " + docElem.tag
                stats.UpdateDocStart()
                for element in docElem.iter():
                    path = tree.getpath(element)
                    #print '>>> path(initial) = "' + path + '"'
                    level = path.count('/') - 1
                    tag = element.tag
                    if not isinstance(tag, str):
                        tag = str(tag)
                    d = dict(element.attrib)
                    if (level >= levelMin) and (len(d) > 0):
                        # print "Update(): len(d) = %d" % len(d)
                        theKeys = d.keys()
                        # print "theKeys = ",
                        # print theKeys
                        tag = tag + '['
                        for myKey in theKeys:
                            tag = tag + myKey + "='" + d[myKey] + "',"
                        tag = tag[:-1] + ']'    # the [:-1] is to omit the last comma
                    # print ('level %d: ' % level) + 'tag = "' + tag + '"'
                    for i in range(level + 1 - self.tagLevelsAllocated):
                        self.tags.append(" ")
                        self.tagLevelsAllocated += 1
                    if True: #level <= self.tagLevels + 1:
                        self.tags[level] = tag
                        self.tagLevels = level + 1
                    else:
                        print "ERROR: level = %d when current level is %d !!!" % (level, self.tagLevels)

                    #print "self.tags =", self.tags
                    if True:
                        listTags = []
                        for sTag in self.tags[:self.tagLevels]:
                            #print 'sTag =', sTag   #, 'type(sTag)=', type(sTag)
                            if isinstance(sTag, str):
                                listTags.append(sTag)
                            else:
                                listTags.append(str(sTag))   #'UNKNOWN')
                        fullTag = "/".join(listTags)
                        #print 'listTags = ', listTags
                        #print 'fullTag = ', fullTag
                    else:
                        fullTag = "".join([(s + '/') for s in self.tags[:self.tagLevels]])

                    #print "FULL TAG = " + fullTag
                    if element.text is None:
                        textLength = 0
                    else:
                        textLength = len(element.text)
                    #print '       textLength = %d' % textLength
                    if fullTag in self.d:
                        #print " IN   "
                        self.d[fullTag].curCount += 1
                        self.d[fullTag].maxTextLength = max(self.d[fullTag].maxTextLength, textLength)
                    else:
                        #print " NOT IN   "
                        self.d[fullTag] = CStat()
                        self.d[fullTag].curCount = 1
                        self.d[fullTag].maxTextLength = textLength
                    # print "LEVELS:",
                    #print self.tags[:self.tagLevels]
                    #print "DICTIONARY: "
                    # print self.d
                    #for dd in sorted(self.d.keys()):
                    #    print "   %2d: %s" % (self.d[dd], dd)
                    #print "---------------------"
                stats.UpdateDocEnd()
                if (iDoc % 1000) == 0:
                    print 'iDoc = %d' % iDoc
                    self.Display()

    def Display(self, bAbbreviate = True):
        print 'iDoc Abbreviate = ',
        print bAbbreviate
        cols = self.d.keys()
        if len(cols) > 0:
            cols.sort()
            if bAbbreviate:
                for col in cols:
                    colShort = re.sub('{.*?}', replaceCurlyBraces, col)
                    colShort = re.sub("'http:/.*?'", "'http:/...'", colShort)
                    print unicodeToString(colShort) + ":",
                    print 'min=%d, max=%2d, total=%d, maxTextLength=%d' % (self.d[col].minCount, self.d[col].maxCount, self.d[col].totalCount, self.d[col].maxTextLength)
            else:
                for col in cols:
                    print unicodeToString(col) + ':',
                    print 'min=%d, max=%2d, total=%d, maxTextLength=%d' % (self.d[col].minCount, self.d[col].maxCount, self.d[col].totalCount, self.d[col].maxTextLength)

#---------------------------------------------------------------------------------------------
class CParser:

    def __init__(self, beautifulSoup = None, bJson = False, bFakeRoot = False):
        self.beautifulSoup = beautifulSoup          # None -> disable BeautifulSoup parsing, other values enable it with the specified parameters)
        self.bJson = bJson
        self.bFakeRoot = bFakeRoot
        self.xmlParser = etree.XMLParser(remove_blank_text = True, recover=True) #lxml.etree only;  recovers from bad characters.
            # we'll need these regexp programs
        self.reProgNewlines = re.compile(r'$', re.MULTILINE)            # end of each line
        self.reProgEndOfFile = re.compile(r'(,\s*)*\Z', re.MULTILINE)   # replace any extra commas before the end of file -those that don't separate non-white-space chars
        #dicttoxml.set_debug(debug=True)

    def BeautifulSoupSet(self, bs):
        self.beautifulSoup = bs

    def JsonSet(self, bJson):
        self.bJson = bJson

    def FakeRootSet(self, bFakeRoot):
        self.bFakeRoot = bFakeRoot

    def Parse(self, theFile):
        # parses a file and returns the tree
        if self.bJson:
            if argsParsed.verbose:
                print '\n JSON parsing.... \n'
                print 'wcc##### reading file...', str(datetime.datetime.now())

            # free memory
            tree = None
            s = None
            j = None
            x = None
            aNode = None
            xList = ['<?xml version="1.0" encoding="UTF-8" ?>\n<UniversalIngesterFakeRoot><UniversalIngesterDocs>']

            bDone = False

            while not bDone:
                listLines = []      # no lines
                bDone = False

                for iLine in range(0, 300):        # read a block of this number of lines each iteration
                    line = theFile.readline()
                    #print '         LINE: \"%s\"'% line
                    if line == '':
                        bDone = True
                        break
                    else:
                        listLines.append(line)
                if len(listLines) > 0:  # we got at least one good line, so we should ingest what we got
                    s = ''.join(listLines)
                    #print 'FINDALL.............', reProgNewlines.findall(s)
                    s = self.reProgNewlines.sub(r',', s)

                    # all commas after the last record and before the end of the last line should be deleted
                    # add the enclosing braces and structure
                    s = '[ ' + self.reProgEndOfFile.sub(r' ]', s)
                    #print 's = '#, s
                    #print '##### substitution', str(datetime.datetime.now())

                    j = json.loads(s)

                    #print '##### json', str(datetime.datetime.now())

                    x = dicttoxml.dicttoxml(j, attr_type=False, ids=False)
                             # remove the heading   <?xml version="1.0" encoding="UTF-8"?>
                    xStartPos = 1 + x.find('>')
                    xList.append(x[xStartPos:])
                    #print 'x appending (startPos = %d) :'% xStartPos, x[xStartPos:]

                    ################################################################
                    #### HACK to clean up some progressive slow-down in the state of this module - probably a memory leak
                    ################################################################
                    reload(dicttoxml)

                    #print '##### xml', str(datetime.datetime.now())

            # done reading the snippets of xml, now merge them and parse them.
            xList.append('</UniversalIngesterDocs></UniversalIngesterFakeRoot>')
            x = '\n'.join(xList)
            #print 'x: ', x
            aNode = etree.fromstring(x, self.xmlParser)
            #print ' XML anode IS *****************************\n', aNode

            tree = aNode.getroottree()

            #print ' XML tree IS *****************************\n', etree.tostring(aNode, pretty_print = True)

        else:
            if self.bFakeRoot:
                xml = theFile.read()
                xml = '<UniversalIngesterFakeRoot>' + xml + '</UniversalIngesterFakeRoot>'
                if self.beautifulSoup is None:
                    aNode = etree.fromstring(xml, self.xmlParser)
                    tree = aNode.getroottree()
                else:
                    soup = BeautifulSoup(xml, self.beautifulSoup)
                    # print(soup.prettify())

                    tree = etree.parse(StringIO.StringIO(soup.prettify()), self.xmlParser)
            else:
                if self.beautifulSoup is None:
                    tree = etree.parse(theFile, self.xmlParser)
                else:
                    soup = BeautifulSoup(theFile, self.beautifulSoup)
                    # print(soup.prettify())
                    tree = etree.parse(StringIO.StringIO(soup.prettify()), self.xmlParser)
        return tree

###############################################################################
def shellCmd(cmd, bForce=False, bOutput=True):
    bPrintOnly = False  # set to true to print, but not execute, commands

    output = ' '
    print '"%s"' % cmd
    if (bPrintOnly != True) or bForce:
        if bOutput:
            output = subprocess.check_output(cmd, shell=True)
        else:
            subprocess.call(cmd, shell=True)
    print output
    return output

###############################################################################
def dbInit(username, password, hostname = None):
    #  use parameters from the default file
    # db = MySQLdb.connect(read_default_file="~/.my.cnf")
    if hostname:
        db = MySQLdb.connect(user = username, passwd = password, host = hostname,
                     charset='utf8', use_unicode = True)  # essential for proper handling of unicode characters
    else:
        db = MySQLdb.connect(user = username, passwd = password,
                     charset = 'utf8', use_unicode = True)  # essential for proper handling of unicode characters


    # create a Cursor object to execute queries
    cur = db.cursor()
    # print db.get_character_set_info()

    return (db, cur)

###############################################################################
def dbCmd(cmd, bForceExecute=False, bOutput=True, bPrint=True):
    bPrintOnly = False  # set to true to print, but not execute, commands
    output = ''

    if bPrint:
        print '"%s"' % cmd

    if (bPrintOnly != True) or bForceExecute:
        cur.execute(cmd)
        if bOutput:
            output = cur.fetchall()
            if bPrint:
                print output

    return output

###############################################################################
# Test / debug function used to create a smaller tree from the raw data - consisting of the first "numNodes" nodes
# is specific to pubmed data.
###############################################################################
def CreateTestXML(infilename, outfilename, numNodes):
    with open(infilename, 'r') as f:
        tree = myParser.Parse(f)

        # strip all but the first few children, and write to a new file
        bads = tree.findall('.//MedlineCitation')[numNodes:]
        print len(bads)
        iBad = 0
        for bad in bads:
            iBad += 1
            print iBad

            bad.getparent().remove(bad)
        remainder = tree.findall('.//MedlineCitation')
        print 'remaining.... ',
        print len(remainder)

        # et = etree.ElementTree(tree)
        # et.write('c:\\share\test.xml')
        tree.write(outfilename)

###############################################################################
# print entire path of each node
def TreePrintPaths(tree):
    for element in tree.iter():
        path = tree.getpath(element)
        level = path.count('/') - 1
        print ('%2d' % level) + ' ' + path
        #print("%s - %s" % (element.tag, element.text))
        #print("%s" % (element.tag))


###############################################################################
# Print just the leaf's node with indentation proportional to depth
# subTreeRoot = the root of the subtree to print - if none is specified, uses tree's root
def TreePrintIndent(tree, subTreeRoot = None, printText = None):
    print "--------------------------------------"
    if subTreeRoot == None:
        subTreeRoot = tree
    if printText == None:
        printText = False
    for element in subTreeRoot.iter():
        path = tree.getpath(element)
        level = path.count('/') - 1
        #print path
        # print 'element = ', element, 'tag = ', element.tag
        print ('%2d' % level) + ('  ' * level) + unicodeToString(element.tag),

        d = dict(element.attrib)
        if len(d) > 0:
            print d.items(),

        if printText and element.text is not None:
            print '         ' + unicodeToString(element.text)
        else:
            print ''

        if element.tail is not None:
            print 'TAIL = "' + element.tail + '"'

        #print 'toString = "' + etree.tostring(element) + '"'

        #print("%s - %s" % (element.tag, element.text))



###############################################################################
# Detects if the database to be created by the control file already exists
def DBExists(dbName):
    dbs = dbCmd('show databases like "%s";' % dbName)
    bExists = (len(dbs) > 0)
    print 'DBExists("' + dbName + '") =',
    print 'True' if bExists else 'False'
    return bExists

###############################################################################
# Create the database and tables
def DBCreate(control):
    myCmd = ''
    dbCmd('DROP DATABASE IF EXISTS %s ;' % control.dbName)       # DROP database if it already exists - start fresh
    dbCmd('CREATE DATABASE %s CHARACTER SET "utf8";' % control.dbName)
    dbCmd('USE %s ;' % control.dbName)

    for table in control.tables:
        myCmd = 'CREATE TABLE ' + table.name + ' ('
        bFirst = True
        for col in table.columns:
            if bFirst:
                bFirst = False
            else:
                myCmd = myCmd + ', '
            myCmd = myCmd + col.name + ' ' + col.type + ' ' + col.dbFlags
        myCmd = myCmd + ') ENGINE = MYISAM;'
        dbCmd(myCmd)

###############################################################################
# return the AUTO_INCREMENT value of the target table in the target database
def DBGetNextDocId(control):
    id = 1
    if control.docIdTable is None:
        print 'ERROR: No docId table specified using tag="DOC_ID" and dbFlags includes "AUTO_INCREMENT"'
    else:
        value = dbCmd('select AUTO_INCREMENT from information_schema.tables where TABLE_NAME="' + control.docIdTable + '"  and TABLE_SCHEMA="' + control.dbName + '";');
        id = int(value[0][0])
    return id

###############################################################################
# Insert a row
def DBTableRowInsert(table, columns, values):
    myCmd = 'INSERT INTO ' + table.name + ' ('

    bFirst = True
    for col in columns:
        if bFirst:
            bFirst = False
        else:
            myCmd = myCmd + ', '
        myCmd = myCmd + col.name

    myCmd = myCmd + ') VALUES ('
    bFirst = True
    for val in values:
        if bFirst:
            bFirst = False
        else:
            myCmd = myCmd + ', '
        if val is None:
            myCmd = myCmd + 'NULL'
        else:
            myCmd = myCmd + val
    myCmd = myCmd + ');'
    #print 'TYPE(\"' + myCmd + '\") = ' + str(__builtins__.type(myCmd))
    dbCmd(myCmd, bOutput=False, bPrint=False)



########################################################################
# USAGE: Use as follows to replace all pairs of curly braces with empty versions
#   re.sub('{.*?}', replaceCurlyBraces, s)
# eg.        "one {test} to TEST"
#    yields  "one {...} to TEST"
#
# eg.        "1{test}2{3}3{4444444}45{567890}"
#    yields  "1{...}2{...}3{...}45{...}"

########################################################################
def replaceCurlyBraces(m):      # m is a matchobj
    #print 'group(0) = ' + m.group(0)
    if m.group(0) == '{': return '{}'
    else: return '{...}'    # this replaces the regexp
########################################################################
def TimePrintNow():
    print str(datetime.datetime.now())


###############################################################################
if __name__ == "__main__":

    TimePrintNow()
    control = CControl()

    argParser = argparse.ArgumentParser()
    argParser.add_argument('controlFilename')
    argParser.add_argument("-append", help="append the existing database (default is to overwrite it)",
                                    action="store_true")
    argParser.add_argument("-find", help="finds text values corresponding to the matching tag")
    argParser.add_argument("-findx", help="finds, using XPath, text values corresponding to the matching tag")
    argParser.add_argument("--hostname", default = None, help = "name of database, default is local",
                                    action = "store")
    argParser.add_argument("-stats", help="STATisticS about corpora",
                                    action="store_true")
    argParser.add_argument("-statsv", help="STATisticS about corpora - Verbose",
                                    action="store_true")
    argParser.add_argument("-u", "--username", default="root", help="username for database",
                                    action="store")
    argParser.add_argument("-p", help="prompt for password for database",
                                    action="store_true")
    argParser.add_argument("--password", default=None, help="password for database specified on command line",
                                    action="store")
    argParser.add_argument("-tree", help="display the XML tree structure, without text values",
                                    action="store_true")
    argParser.add_argument("-treetext", help="display the XML tree structure, WITH text values",
                                    action="store_true")
    argParser.add_argument("-v", "--verbose", help="verbose - prints more debug info",
                                    action="store_true")
    argParser.add_argument("-x", help="eXperiment",
                                    action="store_true")
    argsParsed = argParser.parse_args()

    if argsParsed.password != None:
        password = argsParsed.password
    elif argsParsed.p:
        password = getpass.getpass('Enter password for database: ')
    else:
        password = 'root'   #default

    print '---------------------------------------'
    print 'Control Filename = ' + argsParsed.controlFilename
    shellCmd('ls ' + argsParsed.controlFilename)

    db, cur = dbInit(argsParsed.username, password, argsParsed.hostname)

    myParser = CParser(beautifulSoup = ['html.parser'])  #'html.parser')

    # CreateTestXML("/media/sf_share/medline10n0617.xml", "/media/sf_share/test.xml", 25)

    # CSV file parse
    with open(argsParsed.controlFilename, 'rb') as f:
        print 'FILE %s opened successfully' % argsParsed.controlFilename
        reader = csv.reader(f, delimiter='|')
        for row in reader:
            print 'ROW: ',
            print row
            for iCol in range(len(row)):
                row[iCol] = row[iCol].strip()
                if len(control.defines):
                    for define in control.defines:
                        row[iCol] = row[iCol].replace(define[0], define[1])
            if len(control.defines):
                print 'After substitution: ',
                print row

            for col in row:
                print '    COL: "%s"' % col
            bComment = False
            if row == None or len(row) == 0:
                bComment = True
            else:
                firstChar = row[0][0]
                if firstChar == '#' or firstChar == '/' or firstChar == ';':
                    bComment = True
            if bComment:
                pass # print '.............. COMMENT'
            else:
                cmd = row[0].lower()
                print '### COMMAND = ' + cmd
                if cmd == 'replace':
                    control.defines.append((row[1], row[2]))
                elif cmd == 'database':
                    control.dbName = row[1]
                    control.docStartTag = None
                    if (len(row) > 2):
                        if (len(row[2]) > 0):
                            control.docStartTag = row[2]
                    print 'control.docStartTag = ',
                    print control.docStartTag
                elif cmd == 'beautifulsoup':
                    print 'row len =%d, row = ' % len(row),
                    print row
                    if len(row) < 2 or row[1] == '':
                        control.BeautifulSoupSet(None)
                    else:
                        control.BeautifulSoupSet(row[1:])
                elif cmd == 'json':
                    print 'row len =%d, row = ' % len(row),
                    print row
                    if len(row) >= 2 and row[1].lower() == 'on':
                        control.JsonSet(True)
                    else:
                        control.JsonSet(False)
                elif cmd == 'fakeroot':     # add a fake root node that encompasses entire xml file because there are multiple root nodes
                    print 'row len =%d, row = ' % len(row),
                    print row
                    if len(row) >= 2 and row[1].lower() == 'on':
                        control.FakeRootSet(True)
                    else:
                        control.FakeRootSet(False)
                elif cmd == 'files':
                    for iFile in range(1, len(row)):
                        theFile = row[iFile]
                        print '    ...adding file \"%s\"' % theFile
                        control.SourceFilenameAdd(theFile)
                elif cmd == 'filelist':
                    for iFile in range(1, len(row)):
                        theFile = row[iFile]
                        print '    ...adding file \"%s\"' % theFile
                        control.SourceFileListAdd(theFile)
                elif cmd == 'table':
                    control.TableAdd(row[1], row[2] if (len(row) > 2) else None)
                elif cmd == 'column':
                    name    = row[1]
                    type    = row[2]
                    dbFlags = row[3]
                    tag     = row[4]
                    attrib  = None

                    # if an attribute without a value is specified, then the value we want is the value of that attribute
                    # search for @ followed by the tag name, and then no equal sign before the closing brace and end of string
                    matches = re.findall('@([^=]*?)]$', tag)
                    if len(matches) > 0:
                        attrib = matches[0].strip()

                    # choose a function for extracting the value from the element
                    getValueFunction = GetValueFromElement_Text
                    if len(row) > 5:
                        columnFlags = row[5].split()
                        if 'RECURSE' in columnFlags:
                            getValueFunction = GetValueFromElement_Recurse
                        elif 'NO_CHILDREN' in columnFlags:
                            getValueFunction = GetValueFromElement_NoChildren
                        elif 'RECURSE_NO_TAGS' in columnFlags:
                            getValueFunction = GetValueFromElement_RecurseNoTags
                    table = control.TableGetLast()
                    print '@@@@@@@@ last table is ',
                    print table
                    table.ColumnAdd(CTableColumn(name, type, dbFlags, tag, attrib, getValueFunction))
                    if ('AUTO_INCREMENT' in dbFlags) and (tag == 'DOC_ID'):
                        if control.docIdTable is None:
                            control.docIdTable = table.name
                        else:
                            print 'ERROR: more than one DOC_ID columns specified with AUTO_INCREMENT.'
                else:
                    print 'ERROR: unknown command: ' + cmd

    print 'control:'
    print control

    if argsParsed.stats or argsParsed.statsv:  # display statistics about corpora
        stats = CStats()
        iDoc = -1
        for source in control.sources:
            iFilename = control.SourceFileIter_Init(source)

            myParser.BeautifulSoupSet(source[1])
            myParser.JsonSet(source[2])
            myParser.FakeRootSet(source[3])
            for filename in iFilename:
                if argsParsed.verbose:
                    print 'source = ', source, ', filename = ' + filename
                with open(filename, 'r') as f:
                    iDoc += 1
                    tree = myParser.Parse(f)
                    stats.Update(tree, control, iDoc)
            control.SourceFileIter_Exit(source, iFilename)
        print 'stats:', stats.Display(bAbbreviate = not argsParsed.statsv)

    elif argsParsed.x:    # debug mode
        for source in control.sources:
            iFilename = control.SourceFileIter_Init(source)

            myParser.BeautifulSoupSet(source[1])
            myParser.JsonSet(source[2])
            myParser.FakeRootSet(source[3])
            for filename in iFilename:
                if argsParsed.verbose:
                    print 'source = ', source, ', filename = ' + filename
                with open(filename, 'r') as f:
                    tree = myParser.Parse(f)
                    #etree.cleanup_namespaces(tree)
                    #objectify.deannotate(tree, cleanup_namespaces=True)
                    TreePrintIndent(tree, tree, printText=True)

                    '''
                    for node in tree.findall(".//{http://www.loc.gov/MARC21/slim}."):
                        print node.tag, node.text,
                        print node.attrib.items()

                    for node in tree.findall(".//{http://www.loc.gov/MARC21/slim}."):
                        print node.tag, node.text,
                        print node.attrib.items()
                    '''
            control.SourceFileIter_Exit(source, iFilename)
    elif argsParsed.find is not None or argsParsed.findx is not None:
    # find all instances of this field through all the docs (findx uses XPath)
        for source in control.sources:
            iFilename = control.SourceFileIter_Init(source)

            myParser.BeautifulSoupSet(source[1])
            myParser.JsonSet(source[2])
            myParser.FakeRootSet(source[3])
            for filename in iFilename:
                if argsParsed.verbose:
                    print 'source = ', source, ', filename = ' + filename
                with open(filename, 'r') as f:
                    tree = myParser.Parse(f)
                    # for docElem in tree.findall(argsParsed.find):
                    # for docElem in tree.xpath(argsParsed.findx, namespaces={"re": "http://exslt.org/regular-expressions"}):
                    if argsParsed.find is not None:
                        theElems = tree.findall(argsParsed.find)
                    else:
                        theElems = tree.xpath(argsParsed.findx)
                    for docElem in theElems:
                        print tree.getpath(docElem),
                        if docElem.text:
                            #print docElem.text
                            TreePrintIndent(tree, docElem, printText=True)
            control.SourceFileIter_Exit(source, iFilename)
    elif argsParsed.tree or argsParsed.treetext: # print the tree structure - tags of the first document, and possibly the values/text
        for source in control.sources:
            iFilename = control.SourceFileIter_Init(source)

            myParser.BeautifulSoupSet(source[1])
            myParser.JsonSet(source[2])
            myParser.FakeRootSet(source[3])
            for filename in iFilename:
                if argsParsed.verbose:
                    print 'source = ', source, ', filename = ' + filename
                with open(filename, 'r') as f:
                    tree = myParser.Parse(f)
                    docElems = GetDocElems(tree, control.docStartTag)
                    for docElem in docElems:
                        if argsParsed.treetext:
                            TreePrintIndent(tree, docElem, printText=True)
                        else:
                            TreePrintIndent(tree, docElem, printText=False)
            control.SourceFileIter_Exit(source, iFilename)

    else:

        # TODO: add incremental update capability - don't create DB but add to it
        docId = 1      # start the docId
        if DBExists(control.dbName) and argsParsed.append:
            dbCmd('USE %s ;' % control.dbName)
            docId = DBGetNextDocId(control)
        else:
            DBCreate(control)
        docId += -1         # we use a pre-increment loop below
        tree = None         # free the memory from the last iteration
        docElems = None     # free the memory from the last iteration

        # metadata files
        for source in control.sources:
            iFilename = control.SourceFileIter_Init(source)

            myParser.BeautifulSoupSet(source[1])
            myParser.JsonSet(source[2])
            myParser.FakeRootSet(source[3])

            #del tree   # free the memory from the last iteration
            #del docElems   # free the memory from the last iteration

            TimePrintNow()

            for filename in iFilename:
                if argsParsed.verbose:
                    print 'source = ', source, ', filename = ' + filename

                with open(filename, 'r') as f:
                    if argsParsed.verbose:
                        print "parse Start"

                    tree = myParser.Parse(f)

                    if argsParsed.verbose:
                        print "parse End"
                        TreePrintIndent(tree)
                        print "DONE printing ~~~~~~~~"

                    iDoc = -1
                    docElems = GetDocElems(tree, control.docStartTag)   # iterate over each document
                    #print '................... docElems (%d) =' % len(docElems)
                    #print docElems

                    #if True:
                    #    print '### Skipping actual processing!!!!'
                    #else:   # perform actual processing
                    for docElem in docElems:
                        iDoc += 1
                        docId += 1
                        #print ('%3d' % iDoc) + tree.getpath(docElem)
                        '''if iDoc > 5:
                            break
                        '''

                        # get all the fields needed for each table
                        for table in control.tables:
                            nCols = len(table.columns)
                            #print 'table:  name=%s,  numColumns=%d' % (table.name, nCols)
                            if table.forEachTag:
                                # print '########### ' + table.forEachTag
                                elements = docElem.findall(table.forEachTag)
                                # print  elements
                            else:
                                elements = [docElem]
                            for iElem, elem in enumerate(elements, start = 1):
                                values = []
                                for iCol in range(nCols):
                                    col = table.columns[iCol]

                                    #print 'tag = ' + col.tag
                                    if col.tag == 'DOC_ID':             # DOC_ID:  ID that is unique to each document
                                        values.append(str(docId))
                                    elif col.tag == 'FOREACH_INDEX':    # FOREACH_INDEX:  index of each entry in the "foreach" table
                                        values.append(str(iElem))
                                    elif col.tag == 'FILENAME':   # INPUT_FILENAME:  file currently being ingested
                                        values.append(CleanValue(filename))
                                    elif col.tag == None:               # use this with AUTO_INCREMENT
                                        values.append(None)
                                    else:
                                        elemFound = elem.find(col.tag if control.beautifulSoup is None else col.tagLower)
                                        if elemFound is None:
                                            values.append(None)
                                        else:
                                            if col.attrib != None:
                                                #print 'processing ATTRIB !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
                                                #print 'tag =', col.tag, ', attrib = ', col.attrib, ', elemFound = ', elemFound

                                                d = dict(elemFound.attrib)
                                                try:
                                                    val_pre = d[col.attrib]
                                                    val = GetValueFromElement(val_pre, GetValueFromElement_Identity)
                                                    values.append(val)
                                                except:
                                                    values.append(None)
                                            else:
                                                val = GetValueFromElement(elemFound, col.getValueFunction)
                                                values.append(val)
                                    #print '  col %d. %s  = %s' % (iCol, col.name, values[iCol])
                                # now that we filled in all the values of the columns of the row, insert the row into the table
                                DBTableRowInsert(table, table.columns, values)

                        '''
                        # find all author elements within the subtree of the doc
                        for authorElem in docElem.findall('.//Author'):
                            print '         ' + tree.getpath(authorElem) + ':'
                            for authorSubElem in authorElem.iterchildren():
                                print '                   ' + authorSubElem.tag
                            # print authorElem.keys()
                            # print '         ' + tree.getpath(authorElem) + ':' + authorElem.find('LastName', '') + ';' + authorElem.find('ForeName', '') + ' ' + authorElem.find('Initials', '')
                        '''

                    '''  create a record for each document, maybe each author (first, last, initial) - each time hit end of structure, write and clear it
                    '''
            control.SourceFileIter_Exit(source, iFilename)

            if argsParsed.verbose:
                for table in control.tables:
                    dbCmd('DESCRIBE ' + table.name)
                    dbCmd('SELECT * FROM ' + table.name)
                dbCmd('show databases')
