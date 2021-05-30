# no-sql-benchmarking-with-airflow
This is a benchmarking tool for nosql databases currently supporting mongodb, postgres and neo4j databases and different versions of the same.
Benchamrking process is scheduled using apache airflow.This tool can be extended to include different other databases and their corresponding versions.

## Prerequisites 
To run this tool all you need is a docker installed on your machine.We run docker containers of all the databases and of apache airflow. If you do not have docker installed please install it from here [here](https://www.docker.com/products/docker-desktop). Make sure your docker engine is up and running before you run the airflow.

## Cloning the repository
Use following command to clone the repository
```` bash
git clone https://github.com/Anurag144/no-sql-benchmarking-with-airflow.git
````

## Project structure and files
airflow-docker is the root directory and it has following child directories and files.

**dags** : dags folder has the actual dag file named "benchmarking_dag.py" which is used to schedule a benchmarking task in apache airflow.Furthermore dags folder has different python scripts which does different tasks such as downloading datasets, running queries, storing results etc.

**temp** : temp directory is used to store downloaded dataset which is used for setting up the databases.

**results** : results directory is used to store final bemchmarking results. These results are stored as a csv file.

**dockerfiles** : dockerfiles directory has a dockerfile which is used to get the apache airflow image and run pip commands to install python libraries

**docker-compose.yml** : This docker-compose.yml file contains all the services we need to run the benchmarking.These services are nothing but mongodb, postgres, neo4j and their different versions.


## Running the benchmarking task
Once the docker has been installed and repo has been cloned, open the command prompt and navigate to airflow-docker directory and build the docker compose file using following command.
```` bash
docker-compose up -d --build
````
By this time all the database containers and apache airflow webserver should be up and running.
Apache airflow webserver has been exposed through port number 8081. Access the webserver using localhost and port 8081.
Then run the dag and wait for it to finish. Once finished you will see the resulted csv file in results directory.

##Queries and metrics used for benchmarking

###Queries
- singleRead
- singleWrite
- aggregation
- neighbors
- neighbors2
- neighbors2data
- shortestPath

###Metrics
- Average Execution time
- Average CPU usage
- Average Memory usage

##Configure number of times you want to run each query
By default each query runs for 20 times. Execution time, cpu usage and memory usage are calulated as the average of these 20 runs.
If you want to change this number and want to run queries as many times as you want, just change the following variable in dailyBenchmarking.py file.
```` bash
# queries will be executed that many times and avg exec time,avg cpu and avg memory is returned
number = 20
````

##Add another version of mongodb, postgres or neo4j for benchmarking
Current setup supports two versions of each of three databases. If you want to add new version of any of the databases, just add the service in the docker-compose.yml file and name the container in following format.
"databaseName_versionNumber" for example if you want to add mongodb 4.1, name the container as mongodb_4.1.

Once you add a service in the docker-compose file, add the name of the servie in the following list of respective database in dailyBenchmarking.py file
```` bash
mongodb_container_names = ["mongodb_latest","mongo_4.2"]    # list of mongodb container names
postgres_container_names = ["postgres_latest","postgres_10.0"]    # list of mongodb container names
neo4j_container_names = ["neo4j_latest","neo4j_4.2.5"]    # list of mongodb container names
````