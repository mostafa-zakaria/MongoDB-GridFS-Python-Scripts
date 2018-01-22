import pymongo
import random

#conn = pymongo.MongoClient(host=['localhost:27001', 'localhost:27002', 'localhost:27003'],
#                           replicaSet='lab1')

conn = pymongo.MongoClient(host='localhost', port=27017)

db = conn['Lab2']['objects']

try:

    print("** Inserting Random Values ")

    while True:
        db.insert_one({'contents': random.randint(0, 100000)})

except KeyboardInterrupt:
    print("** Stopping Insert and Closing Connection")
    print('** Collection contains>> {} Documents'.format(db.count()))
    conn.close()

