import datetime
from py2neo import Graph
from py2neo.bulk import create_nodes
import json
import random
import mongodbBenchmarkTest
import os

# setting up the database connection

def create_connection():
    host_address = "airflow-docker_neo4j_container_1"
    #port_no = "7474"
    username = "neo4j"
    password = "test"

    connection_string = "http://"+host_address+"/browser/"

    try:

        driver = Graph(connection_string, user=username, password=password)
        print("Connection to neo4j is established.")
        print(driver)
        return driver

    except Exception as e:
        print("Failed to create the driver:", e)


def insertNodesIntoProfiles():
    with open('./dags/data.json', errors='ignore') as json_data:
        data = json.load(json_data)

    driver = create_connection()

    create_nodes(driver.auto(), data, labels={"profiles"})

def createRelationships():
    driver = create_connection()

    with open('./dags/relations.json', errors='ignore') as json_data:
        data = json.load(json_data)

    _fromList =[d['_from'] for d in data]
    _toList = [d['_to'] for d in data]
    length = len(_fromList)

    for value in range(length):
        my_node = driver.evaluate('MATCH (p:profiles), (f:profiles) WHERE p.user_id = "%s" AND f.user_id = "%s" CREATE (p)-[:HasAFriend]->(f)'%(_fromList[value],_toList[value]))

    #print(my_node)


def deleteAllNodesAndRelationships():

    driver = create_connection()
    driver.delete_all()
    #print("All the nodes and relationships have been deleted.")


def createUserIDList():
    driver = create_connection()

    dataframe_userID = driver.run('MATCH (a:profiles) RETURN a.user_id').to_data_frame()

    userId_list = dataframe_userID.values.tolist()

    randomlist = random.sample(userId_list,2)

    randomTwoUserIDs = [val for sublist in randomlist for val in sublist]

    return randomTwoUserIDs


def singleRead():

    driver = create_connection()
    start_time = datetime.datetime.now()

    my_node = driver.evaluate('MATCH (a:profiles) RETURN a')

    end_time = datetime.datetime.now()
    #print(my_node)
    return (end_time - start_time).total_seconds() * 1000

def singleWrite():
    driver = create_connection()
    start_time = datetime.datetime.now()

    write = driver.run('CREATE (n:profiles) SET n.user_id = 5420, n.AGE = 23')

    end_time = datetime.datetime.now()
    return (end_time - start_time).total_seconds() * 1000

def aggregate():

    driver = create_connection()
    start_time = datetime.datetime.now()

    aggregate = driver.run('MATCH (x:profiles) RETURN sum(toInteger(x.AGE))')

    end_time = datetime.datetime.now()
    #print("Aggregate AGE is : %s"%aggregate)
    return (end_time - start_time).total_seconds() * 1000

def neighbors():

    driver = create_connection()
    randomUserIDList = createUserIDList()
    start_time = datetime.datetime.now()

    result = driver.run('MATCH (s:profiles {user_id:"%s"})-->(n:profiles) RETURN n.user_id'%(randomUserIDList[0])).to_table()

    end_time = datetime.datetime.now()
    #print(f"Neighbors of user_id = {randomUserIDList[0]} are:")
    #print(result)
    return (end_time - start_time).total_seconds() * 1000

def neighbors2():

    driver = create_connection()
    randomUserIDList = createUserIDList()
    start_time = datetime.datetime.now()

    result = driver.run('MATCH (s:profiles {user_id:"%s"})-[*1..2]->(n:profiles) RETURN DISTINCT n.user_id'%(randomUserIDList[0])).to_table()

    end_time = datetime.datetime.now()
    #print(f"Immediate and first level neighbors of user_id = {randomUserIDList[0]} are:")
    #print(result)
    return (end_time - start_time).total_seconds() * 1000

def neighbors2data():

    driver = create_connection()
    randomUserIDList = createUserIDList()
    start_time = datetime.datetime.now()

    result = driver.run('MATCH (s:profiles {user_id:"%s"})-[*1..2]->(n:profiles) RETURN DISTINCT n.user_id, n'%(randomUserIDList[0])).to_table()

    end_time = datetime.datetime.now()
    #print(f"Profiles of neighbors of user_id = {randomUserIDList[0]} are:")
    #print(result)
    return (end_time - start_time).total_seconds() * 1000

def shortestPath():

    driver = create_connection()
    randomUserIDList = createUserIDList()
    start_time = datetime.datetime.now()
    result = driver.run('MATCH (s:profiles {user_id:"%s"}),(n:profiles {user_id:"%s"}), p = shortestPath((s)-[*]->(n)) RETURN [x in nodes(p) | x.user_id] as path'%(randomUserIDList[0],randomUserIDList[1])).to_table()
    end_time = datetime.datetime.now()
    return (end_time - start_time).total_seconds() * 1000

if __name__ == "__main__":
    #create_connection()
    #insertNodesIntoProfiles()
    #createRelationships()
    #deleteAllNodesAndRelationships()
    singleRead()
    #singleWrite()
    #aggregate()
    #neighbors()
    #neighbors2()
    #neighbors2data()
    #shortestPath()
    #createUserIDList()
