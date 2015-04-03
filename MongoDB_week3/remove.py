
# Andrew Erlichson
# MongoDB, Inc. 
# M101P - Copyright 2015, All Rights Reserved


import pymongo
import datetime
import sys

# establish a connection to the database
connection = pymongo.MongoClient("mongodb://localhost")
        
# removes one student
def remove_homework(id, score):

    # get a handle to the students database
    db=connection.school
 
    try:
        result = db.students.update({'_id' : id}, {'$pull' : {'scores' : {'score' : score, 'type' : 'homework'}}})
        print result
    except Exception as e:
        print "Exception: ", type(e), e

def find_doc_to_remove():
    db=connection.school
    try:
        docs = db.students.aggregate([{'$unwind' : "$scores"}, {'$match' : {'scores.type' : "homework"}}, {'$group': {'_id': {'id' :"$_id", 'type' : "$scores.type"}, 'min' : {'$min' : "$scores.score"}}}])
        
        for doc in docs:
            print str(doc['_id']['id']) + " -- "  + str(doc['min'])
            remove_homework(doc['_id']['id'], doc['min'])

    except Exception as e:
        print "Exception: ", type(e), e
        
find_doc_to_remove()

