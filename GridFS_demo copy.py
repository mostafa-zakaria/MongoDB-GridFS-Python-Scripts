import pymongo
import gridfs
from bson.objectid import ObjectId
import time
import os.path

############# functions ############# 

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
    tempFile = open('tempfile_{}'.format(fObj.filename), 'wb')
    tempFile.write(fObj.read())
    tempFile.close()

def putFile(fileName, fs, chunkSize = 255):
    fo = open(fileName, 'rb')
    if(chunkSize == 255):
        fs.put(fo, 
		filename=fileName, 
		type=filename.split('.'),
		steppedPath=fileName.split('/'))
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

############# connection string ############# 

uri = 'mongodb://username:password@serverlist:port/test?ssl=true&replicaSet=rsName&authSource=admin'

############# variables ############# 

db = connect(uri, 27017,'wellData')
fs = gridfs.GridFS(db)
directory = '/data/well'

############# methods ############# 

for filename in os.listdir(directory):
    if filename.endswith(".jpg") or filename.endswith(".png"): 
	#cleanup
	deleteFile(os.path.join(directory, filename), fs)
        #upload
	timeMethod('Time to Upload ' + filename + ' to Atlas>> ', putFile, os.path.join(directory, filename), fs)
        continue
    else:
	continue

############# end ############# 

db.client.close()