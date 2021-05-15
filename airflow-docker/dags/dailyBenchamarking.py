import datetime
import os

import pandas as pd

import mongodbBenchmarkTest,postgresBenchmarkTest,neo4jBenchmarkTest,text_to_json_conversion,downloadPokecDataset

def downloadDatasets():
    #download datasets,unzip, convert to json and remove zip and text files
    downloadPokecDataset.downloadDataset()

def convertDatasetsToJson():
    print("Conversion of from txt to json started..")
    text_to_json_conversion.text_to_json_Profiles_table()
    text_to_json_conversion.text_to_json_Relationship_table()
    print("Conversion of from txt to json finished..")

def setupPostgresDatabase():
    # setup postgres (create db and insert data)
    print("Setting up of postgres database and insertion of data started..")
    postgresBenchmarkTest.create_database()
    postgresBenchmarkTest.create_tables()
    postgresBenchmarkTest.Insert_INTO_profiles_table()
    postgresBenchmarkTest.Insert_INTO_relations_table()
    print("Setting up of postgres database and insertion of data finished..")

def setupMongodbDatabase():
    # setup mongodb (create db and insert data)
    print("Setting up of mongodb database and insertion of data started..")
    mongodbBenchmarkTest.createDB()
    mongodbBenchmarkTest.createProfileCollection()
    mongodbBenchmarkTest.createRelationsCollection()
    mongodbBenchmarkTest.insertIntoProfilesCollection()
    mongodbBenchmarkTest.insertIntoRelationsCollection()
    print("Setting up of mongodb database and insertion of data finished..")

def setupNeo4jDatabase():
    # setup neo4j (create db and insert data)
    print("Setting up of neo4j database and insertion of data started..")
    neo4jBenchmarkTest.insertNodesIntoProfiles()
    neo4jBenchmarkTest.createRelationships()
    print("Setting up of neo4j database and insertion of data finished..")



def singleReadQuery():

    #mongodb read query
    cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeM = datetime.datetime.now()
    exec_timeM = mongodbBenchmarkTest.singleRead()

    #postgres read query
    cpuMemoryListP = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeP = datetime.datetime.now()
    exec_timeP = postgresBenchmarkTest.singleRead()

    #Neo4j read query
    cpuMemoryListN = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeN = datetime.datetime.now()
    exec_timeN = neo4jBenchmarkTest.singleRead()

    #create rows to be inserted into csv
    newRows = {'Attribute': ['Query','DateTime','ExecTime in ms','Memory_Used in %','Cpu_Used in %'],
               'Mongodb' : ['singleRead',current_timeM,format(exec_timeM, ".4f"),format(cpuMemoryListM[1], ".4f"),format(cpuMemoryListM[0], ".4f")],
               'Postgres' :['singleRead',current_timeP,format(exec_timeP, ".4f"),format(cpuMemoryListP[1], ".4f"),format(cpuMemoryListP[0], ".4f")] ,
               'Neo4j': ['singleRead',current_timeN,format(exec_timeN, ".4f"),format(cpuMemoryListN[1], ".4f"),format(cpuMemoryListN[0], ".4f")]}

    #writing to csv file
    dfToCsv = pd.DataFrame(newRows)
    with open('./dags/benchmarkingResults.csv', 'a',newline = '') as f:
        dfToCsv.to_csv(f, index=False,header = False)

def singleWriteQuery():
    # mongodb write query
    cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeM = datetime.datetime.now()
    exec_timeM = mongodbBenchmarkTest.singleWrite()

    # postgres write query
    cpuMemoryListP = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeP = datetime.datetime.now()
    exec_timeP = postgresBenchmarkTest.singleWrite()

    # Neo4j write query
    cpuMemoryListN = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeN = datetime.datetime.now()
    exec_timeN = neo4jBenchmarkTest.singleWrite()

    # create rows to be inserted into csv
    newRows = {'Attribute': ['Query', 'DateTime', 'ExecTime in ms', 'Memory_Used in %', 'Cpu_Used in %'],
               'Mongodb': ['singleWrite', current_timeM, format(exec_timeM, ".4f"), format(cpuMemoryListM[1], ".4f"),
                           format(cpuMemoryListM[0], ".4f")],
               'Postgres': ['singleWrite', current_timeP, format(exec_timeP, ".4f"), format(cpuMemoryListP[1], ".4f"),
                            format(cpuMemoryListP[0], ".4f")],
               'Neo4j': ['singleWrite', current_timeN, format(exec_timeN, ".4f"), format(cpuMemoryListN[1], ".4f"),
                         format(cpuMemoryListN[0], ".4f")]}

    # writing to csv file
    dfToCsv = pd.DataFrame(newRows)
    with open('./dags/benchmarkingResults.csv', 'a', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

def aggregateQuery():
    # mongodb aggregate query
    cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeM = datetime.datetime.now()
    exec_timeM = mongodbBenchmarkTest.aggregate()

    # postgres aggregate query
    cpuMemoryListP = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeP = datetime.datetime.now()
    exec_timeP = postgresBenchmarkTest.aggregate()

    # Neo4j aggregate query
    cpuMemoryListN = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeN = datetime.datetime.now()
    exec_timeN = neo4jBenchmarkTest.aggregate()

    # create rows to be inserted into csv
    newRows = {'Attribute': ['Query', 'DateTime', 'ExecTime in ms', 'Memory_Used in %', 'Cpu_Used in %'],
               'Mongodb': ['aggregate', current_timeM, format(exec_timeM, ".4f"), format(cpuMemoryListM[1], ".4f"),
                           format(cpuMemoryListM[0], ".4f")],
               'Postgres': ['aggregate', current_timeP, format(exec_timeP, ".4f"), format(cpuMemoryListP[1], ".4f"),
                            format(cpuMemoryListP[0], ".4f")],
               'Neo4j': ['aggregate', current_timeN, format(exec_timeN, ".4f"), format(cpuMemoryListN[1], ".4f"),
                         format(cpuMemoryListN[0], ".4f")]}

    # writing to csv file
    dfToCsv = pd.DataFrame(newRows)
    with open('./dags/benchmarkingResults.csv', 'a', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)


def neighborsQuery():
    # mongodb neighbors query
    cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeM = datetime.datetime.now()
    exec_timeM = mongodbBenchmarkTest.neighbors()

    # postgres neighbors query
    cpuMemoryListP = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeP = datetime.datetime.now()
    exec_timeP = postgresBenchmarkTest.neighbors()

    # Neo4j neighbors query
    cpuMemoryListN = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeN = datetime.datetime.now()
    exec_timeN = neo4jBenchmarkTest.neighbors()

    # create rows to be inserted into csv
    newRows = {'Attribute': ['Query', 'DateTime', 'ExecTime in ms', 'Memory_Used in %', 'Cpu_Used in %'],
               'Mongodb': ['neighbors', current_timeM, format(exec_timeM, ".4f"), format(cpuMemoryListM[1], ".4f"),
                           format(cpuMemoryListM[0], ".4f")],
               'Postgres': ['neighbors', current_timeP, format(exec_timeP, ".4f"), format(cpuMemoryListP[1], ".4f"),
                            format(cpuMemoryListP[0], ".4f")],
               'Neo4j': ['neighbors', current_timeN, format(exec_timeN, ".4f"), format(cpuMemoryListN[1], ".4f"),
                         format(cpuMemoryListN[0], ".4f")]}

    # writing to csv file
    dfToCsv = pd.DataFrame(newRows)
    with open('./dags/benchmarkingResults.csv', 'a', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

def neighbors2Query():
    # mongodb neighbors2 query
    cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeM = datetime.datetime.now()
    exec_timeM = mongodbBenchmarkTest.neighbors2()

    # postgres neighbors2 query
    cpuMemoryListP = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeP = datetime.datetime.now()
    exec_timeP = postgresBenchmarkTest.neighbors2()

    # Neo4j neighbors2 query
    cpuMemoryListN = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeN = datetime.datetime.now()
    exec_timeN = neo4jBenchmarkTest.neighbors2()

    # create rows to be inserted into csv
    newRows = {'Attribute': ['Query', 'DateTime', 'ExecTime in ms', 'Memory_Used in %', 'Cpu_Used in %'],
               'Mongodb': ['neighbors2', current_timeM, format(exec_timeM, ".4f"), format(cpuMemoryListM[1], ".4f"),
                           format(cpuMemoryListM[0], ".4f")],
               'Postgres': ['neighbors2', current_timeP, format(exec_timeP, ".4f"), format(cpuMemoryListP[1], ".4f"),
                            format(cpuMemoryListP[0], ".4f")],
               'Neo4j': ['neighbors2', current_timeN, format(exec_timeN, ".4f"), format(cpuMemoryListN[1], ".4f"),
                         format(cpuMemoryListN[0], ".4f")]}

    # writing to csv file
    dfToCsv = pd.DataFrame(newRows)
    with open('./dags/benchmarkingResults.csv', 'a', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

def neighbors2dataQuery():
    # mongodb neighbors2data query
    cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeM = datetime.datetime.now()
    exec_timeM = mongodbBenchmarkTest.neighbors2data()

    # postgres neighbors2data query
    cpuMemoryListP = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeP = datetime.datetime.now()
    exec_timeP = postgresBenchmarkTest.neighbors2data()

    # Neo4j neighbors2data query
    cpuMemoryListN = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeN = datetime.datetime.now()
    exec_timeN = neo4jBenchmarkTest.neighbors2data()

    # create rows to be inserted into csv
    newRows = {'Attribute': ['Query', 'DateTime', 'ExecTime in ms', 'Memory_Used in %', 'Cpu_Used in %'],
               'Mongodb': ['neighbors2data', current_timeM, format(exec_timeM, ".4f"), format(cpuMemoryListM[1], ".4f"),
                           format(cpuMemoryListM[0], ".4f")],
               'Postgres': ['neighbors2data', current_timeP, format(exec_timeP, ".4f"), format(cpuMemoryListP[1], ".4f"),
                            format(cpuMemoryListP[0], ".4f")],
               'Neo4j': ['neighbors2data', current_timeN, format(exec_timeN, ".4f"), format(cpuMemoryListN[1], ".4f"),
                         format(cpuMemoryListN[0], ".4f")]}

    # writing to csv file
    dfToCsv = pd.DataFrame(newRows)
    with open('./dags/benchmarkingResults.csv', 'a', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

def shortestpathQuery():

    # Neo4j shortestpathQuery query
    cpuMemoryListN = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    current_timeN = datetime.datetime.now()
    exec_timeN = neo4jBenchmarkTest.shortestPath()

    # create rows to be inserted into csv
    newRows = {'Attribute': ['Query', 'DateTime', 'ExecTime in ms', 'Memory_Used in %', 'Cpu_Used in %'],
               'Mongodb': ['shortestPath','', '', '',''],
               'Postgres': ['shortestPath','','','',''],
               'Neo4j': ['shortestPath', current_timeN, format(exec_timeN, ".4f"), format(cpuMemoryListN[1], ".4f"),
                         format(cpuMemoryListN[0], ".4f")]}

    # writing to csv file
    dfToCsv = pd.DataFrame(newRows)
    with open('./dags/benchmarkingResults.csv', 'a', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

#
# def createAndRunContainers():
#         createAndRemoveContainers.createContainers()
#
# def stopAndRemoveContainersAndDatasets():
#
#         createAndRemoveContainers.deleteContainers()
#
#         #deleting datasets in json format after setting up the databases with data
#         os.remove("data.json")
#         os.remove("relations.json")

def deleteDatabasesAndDataset():
    #delete mongodb database
    mongodbBenchmarkTest.dropDatabase()

    #delete postgres database
    postgresBenchmarkTest.drop_database()

    #delete postgres database
    neo4jBenchmarkTest.deleteAllNodesAndRelationships()

    #delete datasets
    os.remove("./dags/data.json")
    os.remove("./dags/relations.json")

if __name__ == "__main__":
    #screateAndRunContainers()
    #downloadDatasets()
    #convertDatasetsToJson()
    #setupPostgresDatabase()
    #setupMongodbDatabase()
    #setupNeo4jDatabase()
    #singleReadQuery()
    #singleWriteQuery()
    #aggregateQuery()
    #neighborsQuery()
    #neighbors2Query()
    #neighbors2dataQuery()
    #shortestpathQuery()
    deleteDatabasesAndDataset()
    #stopAndRemoveContainersAndDatasets()