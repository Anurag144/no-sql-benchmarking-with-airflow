import datetime
import os
import time

import pandas as pd

import mongodbBenchmarkTest,postgresBenchmarkTest,neo4jBenchmarkTest,text_to_json_conversion,\
    downloadPokecDataset


# define how many times you want to run each query
# queries will be executed that many times and avg exec time,avg cpu and avg memory is returned
number = 20

exec_time_list = []         # list to store avg exec time
cpu_consumption = []        # list to store avg cpu consumption
memory_consumption = []     # list to store avg memory consumption

mongodb_container_names = ["mongodb_latest","mongo_4.2"]    # list of mongodb container names
postgres_container_names = ["postgres_latest","postgres_10.0"]    # list of mongodb container names
neo4j_container_names = ["neo4j_latest","neo4j_4.2.5"]    # list of mongodb container names

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
    for containerNameM in postgres_container_names:
        postgresBenchmarkTest.create_tables(containerNameM)
        postgresBenchmarkTest.Insert_INTO_profiles_table(containerNameM)
        postgresBenchmarkTest.Insert_INTO_relations_table(containerNameM)
    print("Setting up of postgres database and insertion of data finished..")

def setupMongodbDatabase():
    # setup mongodb (create db and insert data)
    print("Setting up of mongodb database and insertion of data started..")
    for containerNameP in mongodb_container_names:
        mongodbBenchmarkTest.createDB(containerNameP)
        mongodbBenchmarkTest.createProfileCollection(containerNameP)
        mongodbBenchmarkTest.createRelationsCollection(containerNameP)
        mongodbBenchmarkTest.insertIntoProfilesCollection(containerNameP)
        mongodbBenchmarkTest.insertIntoRelationsCollection(containerNameP)
    print("Setting up of mongodb database and insertion of data finished..")

def setupNeo4jDatabase():
    # setup neo4j (create db and insert data)
    print("Setting up of neo4j database and insertion of data started..")
    for containerNameN in neo4j_container_names:
        print("in daily : {}".format(containerNameN))
        neo4jBenchmarkTest.insertNodesIntoProfiles(containerNameN)
        neo4jBenchmarkTest.createRelationships(containerNameN)
    print("Setting up of neo4j database and insertion of data finished..")




def singleReadQuery():
    # mongodb read query
    for containerNameM in mongodb_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.singleRead(containerNameM)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
        number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for mongodb
        newRows = {'Date': [current_timeM],
                   'Database': ['Mongodb'],
                   'Database Version': [containerNameM],
                   'Query': ['singleRead'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # postgres read query

    for containerNameP in postgres_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.singleRead(containerNameP)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for postgres
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [containerNameP],
                   'Query': ['singleRead'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j read query

    for containerNameN in neo4j_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.singleRead(containerNameN)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [containerNameN],
                   'Query': ['singleRead'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

def singleWriteQuery():
    # mongodb write query
    for containerNameM in mongodb_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.singleWrite(containerNameM)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for mongodb
        newRows = {'Date': [current_timeM],
                   'Database': ['Mongodb'],
                   'Database Version': [containerNameM],
                   'Query': ['singleWrite'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # postgres write query
    for containerNameP in postgres_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.singleWrite(containerNameP)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for mongodb
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [containerNameP],
                   'Query': ['singleWrite'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j write query
    for containerNameN in neo4j_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.singleWrite(containerNameN)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [containerNameN],
                   'Query': ['singleWrite'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

def aggregateQuery():
    # mongodb aggregate query
    for containerNameM in mongodb_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.aggregate(containerNameM)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

    # df for mongodb
    newRows = {'Date': [current_timeM],
               'Database': ['Mongodb'],
               'Database Version': [containerNameM],
               'Query': ['aggregate'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # postgres aggregate query
    for containerNameP in postgres_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.aggregate(containerNameP)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for postgres
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [containerNameP],
                   'Query': ['aggregate'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j aggregate query
    for containerNameN in neo4j_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.aggregate(containerNameN)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [containerNameN],
                   'Query': ['aggregate'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)


def neighborsQuery():
    # mongodb neighbors query
    for containerNameM in mongodb_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.neighbors(containerNameM)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for mongodb
        newRows = {'Date': [current_timeM],
                   'Database': ['Mongodb'],
                   'Database Version': [containerNameM],
                   'Query': ['neighbors'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # postgres neighbors query
    for containerNameP in postgres_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.neighbors(containerNameP)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for postgres
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [containerNameP],
                   'Query': ['neighbors'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j neighbors query
    for containerNameN in neo4j_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.neighbors(containerNameN)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [containerNameN],
                   'Query': ['neighbors'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

def neighbors2Query():
    # mongodb neighbors2 query
    for containerNameM in mongodb_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.neighbors2(containerNameM)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for mongodb
        newRows = {'Date': [current_timeM],
                   'Database': ['Mongodb'],
                   'Database Version': [containerNameM],
                   'Query': ['neighbors2'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # postgres neighbors2 query
    for containerNameP in postgres_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.neighbors2(containerNameP)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for postgres
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [containerNameP],
                   'Query': ['neighbors2'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j neighbors2 query
    for containerNameN in neo4j_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.neighbors2(containerNameN)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [containerNameN],
                   'Query': ['neighbors2'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)


def neighbors2dataQuery():
    # mongodb neighbors2data query
    for containerNameM in mongodb_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.neighbors2data(containerNameM)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for mongodb
        newRows = {'Date': [current_timeM],
                   'Database': ['Mongodb'],
                   'Database Version': [containerNameM],
                   'Query': ['neighbors2data'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # postgres neighbors2data query
    for containerNameP in postgres_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.neighbors2data(containerNameP)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for postgres
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [containerNameP],
                   'Query': ['neighbors2data'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j neighbors2data query
    for containerNameN in neo4j_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.neighbors2data(containerNameN)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [containerNameN],
                   'Query': ['neighbors2data'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)


def shortestpathQuery():
    # Neo4j shortestpathQuery query
    for containerNameN in neo4j_container_names:

        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.shortestPath(containerNameN)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [containerNameN],
                   'Query': ['shortestPath'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('/results/benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)



def deleteDatabasesAndDataset():
    #delete mongodb database
    for containerNameM in mongodb_container_names:
        mongodbBenchmarkTest.dropDatabase(containerNameM)

    #delete postgres database
    for containerNameP in postgres_container_names:
        postgresBenchmarkTest.drop_tables('profiles',containerNameP)
        postgresBenchmarkTest.drop_tables('relations',containerNameP)

    #delete neo4j database
    for containerNameN in neo4j_container_names:
        print("in daily delete database container name: {}".format(containerNameN))
        neo4jBenchmarkTest.deleteAllNodesAndRelationships(containerNameN)


    #delete datasets
    #os.remove("/temp/data.json")
    #os.remove("/temp/relations.json")

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