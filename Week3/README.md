# Overview
---

This project involves analyzing the 2022 Green Taxi Trip Record data provided by the New York City Taxi and Limousine Commission (TLC). The main objective is to gain insights into the taxi trips taken in 2022 and understand patterns within the data. To achieve this, we utilize Google Cloud's BigQuery service to create both external and regular tables without employing partitioning or clustering strategies.

## Objectives

- Access and utilize the 2022 Green Taxi Trip Record Parquet Files.
- Create an external table in Google BigQuery to reference the Parquet files stored in a Google Cloud Storage bucket.
- Import the data from the Parquet files into BigQuery by creating a regular table, ensuring no data partitioning or clustering is applied.
- Execute various SQL queries to explore the data, answering specific questions related to trip counts, distinct pickup locations, fare amounts, and optimal table structures for query efficiency.

## Data Source

The data for this project is sourced from the official [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) page, specifically the 2022 Green Taxi Trip Record Parquet Files.

## Tools and Technologies

- **Google Cloud Storage:** Used for storing the Parquet files.
- **Google BigQuery:** Serves as the primary platform for data analysis, allowing us to run SQL queries on large datasets efficiently.
- **SQL:** The main language used for querying and analyzing the data within BigQuery.

This project does not employ data loading orchestration tools such as Mage, Airflow, or Prefect for loading data into BigQuery. Instead, it focuses on direct interactions with BigQuery and Google Cloud Storage.

The following sections of this README document the setup process, SQL queries used, answers to the homework questions, and any insights gained from analyzing the Green Taxi data.
---

#### -> if running mage locally: https://blog.thecloudside.com/mage-ai-the-easy-way-to-automate-your-data-pipelines-1c8b01315eb4

#### -> I did spin up a docker file: docker compose up -d
#### Then mage was leveraged to directly pipe the data from the urls ranging from jan to dec in year 2022
#### Once in the bucket, I continued doing the homework as stated in the homework

#### create "external table" to link data stored in bucket
```sql
CREATE TABLE `dataengineering-411512.ny_taxi.regular_green_taxi_data` AS
SELECT *
FROM `dataengineering-411512.ny_taxi.external_table`;
```
#### import data from external table into a "BQ table" for mor efficient analytics (optimized storage & query capabilites)

--------------------------------------------------------------
#### NOTE: BigQuery table = Regular table = Materialized table
--------------------------------------------------------------


# Homework Questions
---
## ***Question 1:*** What is count of records for the 2022 Green Taxi Data??
- 65,623,481
- 840,402
- 1,936,423
- 253,647

### Query:
```sql
SELECT COUNT(1) FROM `dataengineering-411512.ny_taxi.table_1`;
```

### Output:
840402


## ***Question 2:*** Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 0 MB for the External Table and 6.41MB for the Materialized Table
- 18.82 MB for the External Table and 47.60 MB for the Materialized Table
- 0 MB for the External Table and 0MB for the Materialized Table
- 2.14 MB for the External Table and 0MB for the Materialized Table

#Query:

- ***Materialized table:***
```sql
SELECT COUNT(DISTINCT PULocationID) AS unique_pulocationids
FROM `dataengineering-411512.ny_taxi.regular_green_taxi_data`;
```
  - Output: 6.41MB

- ***External table:***
```sql
SELECT COUNT(DISTINCT PULocationID) AS unique_pulocationids
FROM `dataengineering-411512.ny_taxi.external_table`;
```
  - Output: 0MB


## ***Question 3:*** How many records have a fare_amount of 0?

- 12,488
- 128,219
- 112
- 1,622

- Query:
```sql
SELECT COUNT(1) FROM `dataengineering-411512.ny_taxi.regular_green_taxi_data`
WHERE fare_amount = 0;
```
- Output: 1622


## ***Question 4:*** What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

- Cluster on lpep_pickup_datetime Partition by PUlocationID
- Partition by lpep_pickup_datetime Cluster on PUlocationID
- Partition by lpep_pickup_datetime and Partition by PUlocationID
- Cluster on by lpep_pickup_datetime and Cluster on PUlocationID

- Answer: Partition by lpep_pickup_datetime Cluster on PUlocationID

  - *Partition by lpep_pickup_datetime:*
    - Improves query efficiency by organizing data into segments based on time, allowing BigQuery to quickly access relevant data for time-based queries.

  - *Cluster on PULocationID:*
    - Enhances performance for queries ordered or filtered by location ID by storing rows with the same PULocationID together within each partition.

- Query:
 ```sql
  CREATE OR REPLACE TABLE `dataengineering-411512.ny_taxi.clustered_partitioned_table`
  PARTITION BY DATE(lpep_pickup_datetime)
  CLUSTER BY PULocationID
  AS
  SELECT *
  FROM `dataengineering-411512.ny_taxi.regular_green_taxi_data`;
 ```

## ***Question 5:*** Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

- 22.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table
- 5.63 MB for non-partitioned table and 0 MB for the partitioned table
- 10.31 MB for non-partitioned table and 10.31 MB for the partitioned table

- Query on regularized tabel:
  ```sql
  SELECT DISTINCT PULocationID
  FROM `dataengineering-411512.ny_taxi.regular_green_taxi_data`
  WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
  ```
  - bytes processed: 12.82 MB

- Query on parititioned table:
  ```sql
  SELECT DISTINCT PULocationID
  FROM `dataengineering-411512.ny_taxi.clustered_partitioned_table`
  WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
  ```
  - bytes processed: 1.09 MB


## ***Question 6:*** Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Big Table
- Container Registry

- Answer:
  - GCP Bucket:
    - The data stored in the External Table you created is located in a GCP Bucket. External tables in BigQuery point to data stored outside of BigQuery, such as in Google Cloud Storage (which is organized in "buckets"), allowing to query the data without importing it into BigQuery's managed storage.


## ***Question 7:*** It is best practice in Big Query to always cluster your data:

- True
- False

- Answer:
  - False
  - Reason: In some cases, especially with small datasets or tables that don't benefit from the specific optimizations clustering provides, clustering might not be necessary or beneficial. Hence it is superficial in these scenarios. It may be considered best practice for every scenario just to be on the safe side.

## ***(Bonus: Not worth points) Question 8:***
- No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

- Query:
 ```sql
 SELECT COUNT(*)
 FROM `dataengineering-411512.ny_taxi.regular_green_taxi_data`;
 ```
 - bytes processed: 0 B

 - Reason:

   - **Columnar Storage:**
   Since BigQuery uses a columnar storage approach, when I run a COUNT(*) query, it doesn't have to go through the entire table. It might just look at a bit of metadata or a single column to get the job done, which can really speed things up and cut down on the data it needs to scan.

  - **Table Size Impact:**
   The bigger my table is, the more data there's likely to be scanned, even for a simple count. So, if my table is packed with tons of data, I should expect BigQuery to churn through more bytes to give me the count I asked for.

  - **Smart Optimizations:**
   BigQuery's pretty smart about handling queries. It's got these built-in tricks to make things more efficient. So, when I ask for a total count with COUNT(*), it might pull some clever moves, like using already available metadata, to keep the amount of data it needs to read to a minimum.