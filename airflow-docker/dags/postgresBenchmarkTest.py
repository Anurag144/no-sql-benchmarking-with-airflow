import datetime
import json
import os
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
import psycopg2 as psycopg2
import mongodbBenchmarkTest,neo4jBenchmarkTest

###Check connection

def create_connection():

    # conn = psycopg2.connect("user=postgres,
    #
    #                        host=airflow-docker_postgres_container_1,
    #
    #                        password=secret"")

    conn = psycopg2.connect("host=airflow-docker_postgres_container_1 dbname=postgres user=postgres password=secret")
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True
    print(conn)
    return conn

def create_database():

    try:
        conn = create_connection()
        cur = conn.cursor()
        query = 'CREATE DATABASE test'
        cur.execute(query)

        # close communication with the PostgreSQL database server

        cur.close()

        # commit the changes

        conn.commit()

    except(Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def create_tables():

    try:
        conn = create_connection()
        cur = conn.cursor()
        with open('./dags/data.json', errors='ignore') as json_data:
            data = json.load(json_data)
        columnData = list(data[0].keys())
        queryData = "CREATE TABLE profiles ( " + columnData[0] + " INTEGER PRIMARY KEY"
        for i in range((len(columnData))-1):
            queryData = queryData + "," + columnData[i+1] + " text"
        queryData = queryData + ");"

        cur.execute(queryData)

        with open('./dags/relations.json', errors='ignore') as json_data:
            data = json.load(json_data)
        columnData = list(data[0].keys())
        queryRelations = "CREATE TABLE relations ( " + columnData[0] + " text," + columnData[1] + " text );"


        cur.execute(queryRelations)

        # close communication with the PostgreSQL database server
        cur.close()

        # commit the changes

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()



def drop_database():

    try:
        conn = create_connection()
        cur = conn.cursor()
        query = 'DROP DATABASE test'
        cur.execute(query)

        # close communication with the PostgreSQL database server

        cur.close()

        # commit the changes

        conn.commit()

    except(Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()



def drop_tables(table_name):
    try:
        conn = create_connection()
        cur = conn.cursor()
        query = "DROP TABLE " + table_name + ';'

        cur.execute(query)

        # close communication with the PostgreSQL database server

        cur.close()

        # commit the changes

        conn.commit()

    except(Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:
            conn.close()



def Insert_INTO_profiles_table():

    try:
        #use Python's open() function to load the JSON data
        with open('./dags/data.json',errors='ignore') as json_data:
            data = json.load(json_data)
            query_sql = """ insert into profiles
                    select * from json_populate_recordset(NULL::profiles, %s) """
            conn = create_connection()
            cur = conn.cursor()
            cur.execute(query_sql, (json.dumps(data),))
            conn.commit()

            #print ('\nfinished INSERT INTO profiles table')
            cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:

        if conn is not None:
            conn.close()


def Insert_INTO_relations_table():

    try:
        #use Python's open() function to load the JSON data
        with open('./dags/relations.json',errors='ignore') as json_data:
            data = json.load(json_data)
            query_sql = """ insert into relations
                    select * from json_populate_recordset(NULL::relations, %s) """

            conn = create_connection()
            cur = conn.cursor()
            cur.execute(query_sql, (json.dumps(data),))
            conn.commit()
            #print ('\nfinished INSERT INTO relations table')
            cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()

def singleRead():

    try:
        randomUserIDList = neo4jBenchmarkTest.createUserIDList()
        select_profiles_query = "SELECT * FROM profiles WHERE user_id = '%s'"%(randomUserIDList[0])
        conn = create_connection()
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(select_profiles_query)

        end_time = datetime.datetime.now()
        profiles = cur.fetchall()

        #print(f"Profile of user id {randomUserIDList[0]} ::")
        #print(profiles)
        cur.close()

        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def singleWrite():

    try:
        select_profiles_query = "INSERT INTO profiles (user_id, AGE) VALUES (%s,%s)"
        record_to_insert = ('5320', '23')
        conn = create_connection()
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(select_profiles_query,record_to_insert)

        end_time = datetime.datetime.now()
        cur.close()
        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def Read_relationship_table():

    try:
        select_relationship_query = "SELECT * FROM relations"
        conn = create_connection()
        cur = conn.cursor()
        cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        start_time = datetime.datetime.now()


        cur.execute(select_relationship_query)

        end_time = datetime.datetime.now()
        execTime = (end_time - start_time).total_seconds() * 1000
        relations = cur.fetchall()
        print("Table contents after insertion ::")
        print(relations)
        print("Query execution time = %s Milliseconds" % execTime)
        print(f"CPU used = {cpuMemoryList[0]:.4f}%")
        print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def aggregate():

    try:
        aggregate_query = "select AGE, count(*) from profiles group by AGE"
        conn = create_connection()
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(aggregate_query)

        end_time = datetime.datetime.now()
        age = cur.fetchall()
        #print("Aggregate of AGE ::")
        #print(age)
        cur.close()
        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()

def neighbors():

    try:
        randomUserIDList = neo4jBenchmarkTest.createUserIDList()
        neighbors_query = "SELECT DISTINCT _to FROM relations WHERE _from = '%s'"%(randomUserIDList[0])
        conn = create_connection()
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(neighbors_query)

        end_time = datetime.datetime.now()
        relations = cur.fetchall()

        #print(f"Neighbors of user_id = {randomUserIDList[0]} are:")
        #print(relations)
        cur.close()
        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def neighbors2():

    try:
        randomUserIDList = neo4jBenchmarkTest.createUserIDList()
        neighbors2_query = "select _to from relations where _from = '%s' union distinct select _to from relations" \
                          " where _to != '%s' and _from in (select  _to from relations where _from = '%s')"%(randomUserIDList[0],randomUserIDList[0],randomUserIDList[0])
        conn = create_connection()
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(neighbors2_query)

        end_time = datetime.datetime.now()
        execTime = (end_time - start_time).total_seconds() * 1000
        relations = cur.fetchall()

        #print(f"Immediate and first level neighbors of user_id = {randomUserIDList[0]} are:")
        #print(relations)
        cur.close()
        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()

def neighbors2data():

    try:
        randomUserIDList = neo4jBenchmarkTest.createUserIDList()
        neighbors2data_query = "select * from profiles where user_id::text IN (select _to from relations where _from = '%s' union" \
                           " distinct select _to from relations where _to != '%s' and _from IN" \
                           " (select  _to from relations where _from = '%s'))"%(randomUserIDList[0],randomUserIDList[0],randomUserIDList[0])
        conn = create_connection()
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(neighbors2data_query)

        end_time = datetime.datetime.now()
        relations = cur.fetchall()

        #print(f"Profiles of neighbors of user_id = {randomUserIDList[0]} are:")
        #print(relations)
        cur.close()
        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


if __name__ == "__main__":
    #create_database()
    #create_tables()
    #drop_tables('profiles')
    #drop_tables('relations')
    drop_database()
    #Insert_INTO_profiles_table()
    #Insert_INTO_relations_table()
    #singleRead()
    #singleWrite()
    #Read_relationship_table()
    #neighbors()
    #neighbors2()
    #neighbors2data()
    #aggregate()
