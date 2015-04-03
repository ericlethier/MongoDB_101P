
# Andrew Erlichson
# MongoDB, Inc. 
# M101P - Copyright 2015, All Rights Reserved


import pymongo
import datetime
import sys

# establish a connection to the database
connection = pymongo.MongoClient("mongodb://localhost")
        
# removes one student
def remove_homework(student_id, score):

    # get a handle to the students database
    db=connection.students
    grades = db.grades
 
    try:
        result = grades.remove({'student_id' : student_id, 'score' : score})

    except Exception as e:
        print "Exception: ", type(e), e

def find_doc_to_remove():
    db=connection.students
    grades = db.grades
    
    try:
        docs = db.grades.aggregate([{'$match' : {'type' : "homework"}}, {'$group': {'_id' : "$student_id", 'min': {'$min': "$score"}}}])
        
        for doc in docs:
            remove_homework(doc['_id'], doc['min'])
        
    except Exception as e:
        print "Exception: ", type(e), e
        

def count_homework():
    # get a handle to the students database
    db=connection.students
    grades = db.grades
 #   try: 
    count_hw = grades.find().count()
		
#    except Exception as e:
#        print "Exception: ", type(e), e
	
    print "nb of hw: " + str(count_hw)

count_homework()
find_doc_to_remove()
count_homework()

