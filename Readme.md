## Healthcare Stroke Data Analysis with DSE Analytics

## Project Objective

- Learn to create data models using .cql files 
- Load data to keyspace/table in DSE Core using COPY Feature
- Create a Spark Application that analysis stroke data.

### DATA MODEL 

The data model for the healtcare dataset is provided below.
Create `vi health_load.cql` file and copy the below model.

`CREATE TABLE keyspace1.health_stroke (
     id int,
     gender text,
     age int,
     avg_glucose_level float,
     bmi float,
     ever_married text,
     heart_disease int,
     hypertension int,
     residence_type text,
     smoking_status float,
     stroke int,
     work_type text,
     PRIMARY KEY (id, gender, age)
 ) WITH CLUSTERING ORDER BY (gender ASC, age ASC)`

Create this data model by running `cqlsh -f health_load.cql` from the working directory.

Open `cqlsh` and run `COPY keyspace1.health_stroke FROM 'health_care_stroke.csv' and Header = TRUE`

### LOAD DATA TO DSE CORE

There are various ways to load data into cql from csv. The popular one's are `COPY` and `DSBULK`. For production purposes and for large dataset loading use `dsbulk`.
Since the data set for this demo is small, we are using `COPY` feature. 

Open `cqlsh` and run `COPY keyspace1.health_stroke FROM 'health_load.csv' with DELIMITER=',' AND Header = TRUE`

### CREATE SPARK APPLICATION 

The `SparkHealthApp.Scala` file creates the app and provides few basic analytics results from the cassandra (DSE Core - healths stroke table).

Run `sbt clean` and `sbt package` to get the `health-care-stroke-analytics-dse-demo_2.11-0.1.jar` file. 

The jar file is what we provide along with `dse spark submit` for running the app. 

### Run the SPARK Application on Cluster

Go to the directory where the Jar file is kept or provide the path of the Jar File and run `dse spark-submit --class HealthPack.SparkHealthApp health-care-stroke-analytics-dse-analytics-demo_2.11-0.1.jar` on the cluster.

Note : we do not need to provide a spark master with DSE as it is chosen automatically. 

 #### Dataset : https://www.kaggle.com/asaumya/healthcare-dataset-stroke-data#train_2v.csv