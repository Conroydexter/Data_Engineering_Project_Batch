# Data Engineering Project Batch_Processing 

This batch-processing data application for a data-intensive machine learning application is based on:
A team of researchers from a network of hospitals seek to investigate the average billing amount for patients admitted to hospitals within the network executed in two phases. In phase one, the focus of this project
the aim is to gain insight into how the hospitals are charging for various medical conditions. The project is executed by:
-Reading data from a of CSV file containing billing amount data the hospital issues to a patient on the date of discharge, acquired from https://www.kaggle.com/code/abhishekbhandari17/healthcare-dataset.
-Processes the data records by aggregating the average bill amount by medical condition and hospital to a monthly average.
-Incrementally loading processed data records to a database.
-Reads the processed data records, then adds a label to each record to indicate a billing amount category for each hospital in each quarter via the KMeans Machine Learning model which clusterizes the data records.
-The clusterized data records are then loaded to the database quarterly.
-For visualization the processed and clusterized data records are presented via HTML web pages.

# Project Architecture 
![Project_DE_Architecture](https://github.com/user-attachments/assets/4c6d0b70-6218-409c-adb9-59206c54c1fe)


# Project Implementation
The implementation of the project is facilitated via the deployment of four docker containers:

- The Extract, Transform and Load process: has a data pipeline that is managed and executed through a Spark session with the conbination of Spark and Python (Pyspark), the ETL process executes the job based on a schedule to incrementally add new data with a monthly run.
- PostgreSQL database is combined with a docker named volume to persist data outside the container, with port 5432 exposed for external access.
- The Machine Learning process: through the Spark session the extraction of the processed data and the execution of the ML model clusterizies the data, sets the process to a quarterly schedule that labels the data and regenerates the table.
- Visualization: Utalizing Pyspark and the Bokeh library in a Spark session HTML pages containing the processed data from ETL and ML processes are created. To display the pages, the Nginx service is used through port 80.


# Note:
The ML process is executed upon the successful completion of ETL process and both processes depend on the proper running of the database container. The status table controls the execution between the containers
and the use of the docker network facilities the communication between the containers.


# Running The Project
Note: Docker and Docker-compose are pre-requisites to execute the project.

# Execution Steps 

- i Clone the GitHub repository:

git clone https://github.com/Conroydexter/Batch-Data-Engineering-Project-project-.git

- ii Go into the cloned repository folder:

cd data_engineering_project

- iii Execute the following docker-compose command to build the docker images:

docker-compose build

- iv Execute the following docker-compose command to start the containers:

docker-compose up -d

(Run docker ps to monitor the status of the containers)

# Docker App

![image](https://github.com/user-attachments/assets/4f0c1e4d-f2ba-4474-9875-a999602a100b)


- v Varification of the results via localhost by clicking on the links to the individual pages on the home page or the browser links in the dockerfiles.

Note: Refresh the pages for the most updated information and allow the process a few minutes. Additionally both ETL and ML processes are executed at 15 minutes intervals in test mode,  but production mode can be enabled by following the steps documented in ETL and ML.

- vi The following docker-compose command stops the application:

docker-compose down 

# Documentation
Each of the processes has included README files detailling the execution step:
- Status
- ETL Process
- ML Process
