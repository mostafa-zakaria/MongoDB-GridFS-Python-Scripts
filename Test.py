import pymongo
import gridfs
from bson.objectid import ObjectId
import time
import os.path

def connect(hostName, port, dbName, colName = ''):
    conn = pymongo.MongoClient(host=hostName, port=port)
    if(colName == ''):
        return(conn[dbName])
    else:
        return(conn[dbName][colName])

def timeMethod(message, function, *args):
    start = time.time()
    c = function(*args)
    end = time.time()
    print('{0} {1}'.format(message, end - start))
    return c

def extractFile(fObj):
    tempFile = open(os.path.join('/Users/mostafa.zakaria/GridFS', 'tempfile_{}'.format(fObj.filename)), 'wb')
    #tempFile = open('tempfile_{}'.format(fObj.filename), 'wb')
    tempFile.write(fObj.read())
    tempFile.close()

def putFile(fileName, fs, chunkSize = 255):
    fo = open(fileName, 'rb')
    if(chunkSize == 255):
        fs.put(fo, filename=fileName)
    else:
        fs.put(fo, filename=fileName, chunk_size=chunkSize)

def getFile(fileName, fs):
    if(fs.exists({'filename' : fileName})):
        cursor = timeMethod('Time to Download File/s from Atlas>> ', fs.find, {'filename' : fileName})
        for fObj in cursor:
            timeMethod('Time to Extract and Assemble File Locally>> ', extractFile, fObj)
    else:
        print("File>> {} does not exist".format(fileName))

def deleteFile(fileName, fs):
    c = fs.find({'filename' : fileName})
    for fObj in c:
        fs.delete(ObjectId(fObj._id))

uri = 'mongodb+srv://mzakaria:mxyzptlk@cluster0-yygrq.mongodb.net/test'

db = connect(uri, 27017,'gfs')
fs = gridfs.GridFS(db)

#timeMethod('Time to Upload File to Atlas>> ', putFile, 'ESTA-Status-Summary.pdf', fs)

getFile('ESTA-Status-Summary.pdf', fs)

#timeMethod('Time to Delete File from Atlas>>', deleteFile, 'ESTA-Status-Summary.pdf', fs)

db.client.close()