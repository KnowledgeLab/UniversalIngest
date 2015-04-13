# UniversalIngest

read full document [here](http://www.knowledgelab.org/docs/UniversalIngester-UsersGuide-v1.1.pdf)

## Introduction

The purpose of the Universal Ingester is to “ingest”, or load, data from various sources into our databases. In most cases, the data is bibliographical data related to a corpus of publications (eg. US Patent, Medline/Pubmed, arXiv). To ingest a corpus, the user first generates a “control file” which specifies which data to load, where to find the data, and specifically how to store the data in the database. The Ingester supports the generation of the control file by providing the user with relevant information about the corpus, such as what data is available and where. While Version 1.0 of the Universal Ingester supports metadata in XML format, additional formats will be supported over time (eg. JSON). Once the control file is created, the ingester, which is a python script, is executed, with the control file specified on the command line. A successful execution results in all of the metadata being ingested into the specified database. Unicode (UTF­8) is supported, and is the default format of tables and data.


## Usage

```bash
UniversalIngest.py [­append] [­find <FIND>] [­h] [­stats[v]] [­tree[text]] [­v] controlFilename
```

- `-append` appends data to existing database, if present. Otherwise, overwrites.
- ­`-find <FIND>`` finds the text values corresponding to the tag `<FIND>` for all records
- `-h` prints the usage message
- ­`-statsv` displays Verbose STATisticS about each field that occurs in the corpora (after each chunk of 10000 records processed), including:
  - `min`: minimum number of occurrences of this field per record
  - `max`: maximum number of occurrences of this field per record
  - `total`: total number of occurrences of this field across all records
  - `maxTextLength`: maximum length of text occurring in the field

- `-­tree` displays the structure of the first record in the corpus (just the XML tags and structure)
- `-­treetext` displays the structure of the first record in the corpus with the text values
- `-­v verbose` ­ prints more messages
- `controlFilename` ­the name of the control file


## Control File Format

The control file is a CSV file that uses the pipe, or `|`, character as a delimiter. Commands are case­insensitive, but capitalized here to distinguish them from other words. All other fields are case­sensitive. White space at the start or end of each field is ignored.

Tags refer to the XML tags in the metadata files. Tags are specified in XPATH format. `.` refers to the “current” node ­ which depends on the context, and will be explained in each case. `.//dog` refers to all children nodes of the “current” node that has the tag “dog”.

### Example control file

```bash
DATABASE | myJournal | docTag
FILES | /glusterfs/users/metaknowledge/rawdata/journal/metadata.xml
FILES | /glusterfs/users/metaknowledge/rawdata/journal/metadata2.xml
REPLACE | {NS} | {http://some.crazy.long.unreadable.namespace}
# the docID is AUTO_INCREMENTed and has no tag from which data is extracted
TABLE | docs
COLUMN | docID | INT | AUTO_INCREMENT | DOC_ID
COLUMN | date | DATE | | dateTag
# Each document can have 0 or more authors.
# DOC_ID matches the value in the “docs” table.
# Each author appears as an <author> tag, and includes a <lastname> and <firstname> tag
# Authors are ranked by order of appearance, starting with 1 ­ stored in “rank” column.
TABLE | authors | .//{NS}author
COLUMN | docID | INT | | DOC_ID
COLUMN | rank | INT(2) | | FOREACH_INDEX
COLUMN | LastName | VARCHAR(64) | | .//lastname
COLUMN | FirstName | VARCHAR(64) | | .//firstnames
```

### Members

- [William Catino](http://www.knowledgelab.org/people/detail/william_catino/), Knowledge Lab, University of Chicago

### License

(c) 2015 William Catino
