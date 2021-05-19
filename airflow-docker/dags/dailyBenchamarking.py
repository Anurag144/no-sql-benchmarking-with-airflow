import datetime
import os

import pandas as pd

import mongodbBenchmarkTest,postgresBenchmarkTest,neo4jBenchmarkTest,text_to_json_conversion,\
    downloadPokecDataset


# define how many times you want to run each query
# queries will be executed that many times and avg exec time,avg cpu and avg memory is returned
number = 20

exec_time_list = []         # list to store avg exec time
cpu_consumption = []        # list to store avg cpu consumption
memory_consumption = []     # list to store avg memory consumption

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
    current_timeM = datetime.datetime.now()

    for i in range(number):

        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = mongodbBenchmarkTest.singleRead()
        exec_time_list.append(exec_timeM)
        cpu_consumption.append(cpuMemoryListM[0])
        memory_consumption.append(cpuMemoryListM[1])

    avg_exec_time = sum(exec_time_list) / len(exec_time_list)
    avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
    avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
    print("Average execution time by running the query %s times is: %s milliseconds" % (number,'{:.4f}'.format(avg_exec_time) ))
    print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
    print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

    #df for mongodb
    newRows = {'Date' : [current_timeM],
                'Database' : ['Mongodb'],
                'Query' : ['singleRead'],
                'ExecTime' : [format(avg_exec_time, ".4f")],
                'Memory_Used' : [format(avg_memory_consumption, ".4f")],
                'Cpu_Used' : [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
         dfToCsv.to_csv(f, index=False, header=False)

    #postgres read query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = postgresBenchmarkTest.singleRead()
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
               'Query': ['singleRead'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    #Neo4j read query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = neo4jBenchmarkTest.singleRead()
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
               'Query': ['singleRead'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

def singleWriteQuery():
    # mongodb write query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = mongodbBenchmarkTest.singleWrite()
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
               'Query': ['singleWrite'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # postgres write query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = postgresBenchmarkTest.singleWrite()
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
               'Query': ['singleWrite'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j write query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = neo4jBenchmarkTest.singleWrite()
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
               'Query': ['singleWrite'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

def aggregateQuery():
    # mongodb aggregate query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = mongodbBenchmarkTest.aggregate()
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
               'Query': ['aggregate'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # postgres aggregate query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = postgresBenchmarkTest.aggregate()
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
               'Query': ['aggregate'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j aggregate query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = neo4jBenchmarkTest.aggregate()
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
               'Query': ['aggregate'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)


def neighborsQuery():
    # mongodb neighbors query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = mongodbBenchmarkTest.neighbors()
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
               'Query': ['neighbors'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # postgres neighbors query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = postgresBenchmarkTest.neighbors()
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
               'Query': ['neighbors'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j neighbors query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = neo4jBenchmarkTest.neighbors()
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
               'Query': ['neighbors'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

def neighbors2Query():
    # mongodb neighbors2 query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = mongodbBenchmarkTest.neighbors2()
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
               'Query': ['neighbors2'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # postgres neighbors2 query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = postgresBenchmarkTest.neighbors2()
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
               'Query': ['neighbors2'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j neighbors2 query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = neo4jBenchmarkTest.neighbors2()
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
               'Query': ['neighbors2'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)


def neighbors2dataQuery():
    # mongodb neighbors2data query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = mongodbBenchmarkTest.neighbors2data()
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
               'Query': ['neighbors2data'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # postgres neighbors2data query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = postgresBenchmarkTest.neighbors2data()
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
               'Query': ['neighbors2data'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j neighbors2data query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = neo4jBenchmarkTest.neighbors2data()
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
               'Query': ['neighbors2data'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)


def shortestpathQuery():

    # Neo4j shortestpathQuery query
    current_timeM = datetime.datetime.now()

    for i in range(number):
        cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        exec_timeM = neo4jBenchmarkTest.shortestPath()
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
               'Query': ['shortestPath'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('/results/benchmarkingResults.csv', 'a+', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)



def deleteDatabasesAndDataset():
    #delete mongodb database
    mongodbBenchmarkTest.dropDatabase()

    #delete postgres database
    postgresBenchmarkTest.drop_database()

    #delete postgres database
    neo4jBenchmarkTest.deleteAllNodesAndRelationships()

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