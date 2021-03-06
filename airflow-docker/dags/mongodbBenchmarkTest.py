import datetime
import json
import neo4jBenchmarkTest
from pymongo import MongoClient
import psutil


#------------- Create db connection and create database ----------------------------

def create_connection(containerName):
    #host_address = "127.0.0.1"
    #port_no = "27017"
    containerName = containerName
    username = "mongo"
    password = "secret"

    try:
        conn_string = "mongodb://"+username+":"+password+"@"+containerName+"/" # <----enter username,pwd,host address and port no accordingly
        myclient = MongoClient(conn_string)
        print("Connected successfully!!!")
        #print(myclient)
    except:
        print("Could not connect to MongoDB")

    return myclient

def createDB(containerName):
    containerName = containerName
    myclient = create_connection(containerName)

    dblist = myclient.list_database_names()
    if "test" in dblist:
        print("The test database exists.")
    else:
        mydb = myclient["test"]

    myclient.close()

def createProfileCollection(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    profilesCollection = db["profiles"]
    #print(db)
    myclient.close()

def createRelationsCollection(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    relationsCollection = db["relations"]
    #print(db)
    myclient.close()

def insertIntoProfilesCollection(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    profileCol = db.profiles
    # Loading or Opening the json file
    with open('/temp/data.json', errors='ignore') as json_data:
        data = json.load(json_data)
    # Inserting the loaded data in the Collection
    # if JSON contains data more than one entry
    # insert_many is used else inser_one is used
    if isinstance(data, list):
        profileCol.insert_many(data)
    else:
        profileCol.insert_one(data)

    myclient.close()

def insertIntoRelationsCollection(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    relationsCol = db.relations
    # Loading or Opening the json file
    with open('/temp/relations.json', errors='ignore') as json_data:
        data = json.load(json_data)
    # Inserting the loaded data in the Collection
    # if JSON contains data more than one entry
    # insert_many is used else inser_one is used
    if isinstance(data, list):
        relationsCol.insert_many(data)
    else:
        relationsCol.insert_one(data)

    myclient.close()

def readProfilesCollection(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    mycol = db.profiles
    for x in mycol.find():
        print(x)

    myclient.close()

def readRelationsCollection(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    mycol = db.relations
    for x in mycol.find():
        print(x)
    myclient.close()


def dropProfilesCollection(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    mycol = db.profiles
    mycol.drop()
    #print("profiles connection has been deleted.")
    myclient.close()

def dropRelationsCollection(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    mycol = db.relations
    mycol.drop()
    #print("relations connection has been deleted.")
    myclient.close()

def dropDatabase(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    myclient.drop_database('test')
    print("Test database has been deleted.")
    myclient.close()

def singleRead(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    mycol = db.profiles
    randomUserIDList = neo4jBenchmarkTest.createUserIDList()
    start_time = datetime.datetime.now()
    x = mycol.find_one({"user_id": randomUserIDList[0]})
    end_time = datetime.datetime.now()
    # print(x)
    myclient.close()
    return (end_time - start_time).total_seconds() * 1000

def singleWrite(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    mycol = db.profiles
    randomUserIDList = neo4jBenchmarkTest.createUserIDList()
    start_time = datetime.datetime.now()
    singleWriteResult = mycol.update_one({"user_id": randomUserIDList[0]}, {"$set": {"AGE": randomUserIDList[1]}})
    start_time = datetime.datetime.now()
    end_time = datetime.datetime.now()
    myclient.close()
    return (end_time - start_time).total_seconds() * 1000

def aggregate(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    mycol = db.profiles
    start_time = datetime.datetime.now()

    aggregate_result = mycol.aggregate([{
                                        "$group":
                                            {
                                                "_id" : "&AGE",
                                                "Aggregate Age" : {"$sum":1}
                                            }
                                        }])
    end_time = datetime.datetime.now()
    #for i in aggregate_result:
    #    print(i)
    myclient.close()
    return (end_time - start_time).total_seconds() * 1000

def neighbors(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    mycol = db.relations
    randomUserIDList = neo4jBenchmarkTest.createUserIDList()
    start_time = datetime.datetime.now()

    record = mycol.find({"_from" : randomUserIDList[0]},{"_to": 1})

    end_time = datetime.datetime.now()
    #print(f"Neighbors of user_id = {randomUserIDList[0]} are:")
    #for doc in record:
    #    print(doc)
    myclient.close()
    return (end_time - start_time).total_seconds() * 1000

def neighbors2(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    mycol = db.relations
    #this is sql query
    # sql_query = "select _to from relations where _from = '15' union" \
    #             " distinct select _to from relations where _to != '15' " \
    #             "and" \
    #             " _from in (select  _to from relations where _from = '15')"
    randomUserIDList = neo4jBenchmarkTest.createUserIDList()
    start_time = datetime.datetime.now()
    #result of IN operator
    array = mycol.find({"_from" : randomUserIDList[0]},{"_to" : 1,"_id":0})
    list_IN = []

    for x in array :
        list_IN.append(x["_to"])
    record = mycol.find({ "$and" : [{"$or": [{ "_to " : { "$ne":  randomUserIDList[0] }  },{ "_from" :  randomUserIDList[0]}] },
                                    {"_from": {"$in": list_IN}}]},{"_to": 1 })


    end_time = datetime.datetime.now()
    #print(f"Immediate and first level neighbors of user_id = {randomUserIDList[0]} are:")
    #for doc in record:
    #    print(doc)
    myclient.close()
    return (end_time - start_time).total_seconds() * 1000


def neighbors2data(containerName):
    containerName = containerName
    myclient = create_connection(containerName)
    db = myclient.test
    mycol = db.relations

    #this is a sql query
    # neighbors2data_query = "select * from profiles where user_id::text IN (select _to from relations where _from = '20' union" \
    #                        " distinct select _to from relations where _to != '20' and _from IN" \
    #                        " (select  _to from relations where _from = '20'))"


    #result of IN operator
    randomUserIDList = neo4jBenchmarkTest.createUserIDList()
    array = mycol.find({"_from" : randomUserIDList[0]},{"_to" : 1,"_id":0})
    list_IN = []
    for x in array :
        list_IN.append(x["_to"])
    start_time = datetime.datetime.now()
    innerQuery = mycol.find({ "$and" : [{"$or": [{ "_to " : { "$ne":  randomUserIDList[0] }  },{ "_from" :  randomUserIDList[0]}] },
                                    {"_from": {"$in": list_IN}}]},{"_to": 1 })
    list_inner_query = []
    for x in innerQuery :
        list_inner_query.append(x["_to"])
    #print(list_inner_query)

    mycol_profiles = db.profiles
    mainQuery = mycol_profiles.find({"user_id":{"$in" : list_inner_query}})

    end_time = datetime.datetime.now()
    #print(f"Profiles of neighbors of user_id = {randomUserIDList[0]} are:")
    #for doc in mainQuery:
    #   print(doc)
    myclient.close()
    return (end_time - start_time).total_seconds() * 1000

def calculateCPUandMemoryUsage(pid):
    process = psutil.Process(pid)
    cpuMemoryList = [process.cpu_percent(interval=0.1),process.memory_percent()]
    return cpuMemoryList

if __name__ == "__main__":
    #createDB()
    #createProfileCollection()
    #createRelationsCollection()
    #insertIntoProfilesCollection()
    #insertIntoRelationsCollection()
    #readProfilesCollection()
    #readRelationsCollection()
    #dropProfilesCollection()
    #dropRelationsCollection()
    dropDatabase()
    #singleRead()
    #singleWrite()
    #aggregate()
    #neighbors()
    #neighbors2()
    #neighbors2data()
    #loo()











