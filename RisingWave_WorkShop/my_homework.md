Q0) What are the dropoff taxi zones at the latest dropoff times?


a) Monitoring or analyzing the latest activity across all zones:

```sql
    CREATE MATERIALIZED VIEW latest_dropoff_per_zone AS
        SELECT
            tz.zone,
            MAX(td.tpep_dropoff_datetime) AS latest_dropoff_time
        FROM
            trip_data td
        JOIN
            taxi_zone tz ON td.dolocationid = tz.location_id
        GROUP BY
            tz.zone
        ORDER BY
            latest_dropoff_time DESC;
```

b) Identifying where the most recent dropoff event occurred:

```sql
    CREATE MATERIALIZED VIEW latest_dropoff_zone AS
    SELECT
        tz.Zone AS taxi_zone,
        td.tpep_dropoff_datetime AS latest_dropoff_time
    FROM
        trip_data td
    JOIN
        taxi_zone tz ON td.DOLocationID = tz.location_id
    WHERE
        td.tpep_dropoff_datetime = (
            SELECT MAX(tpep_dropoff_datetime)
            FROM trip_data
        );
```

**SOLUTION:**
Midtown Center

Q1) Create a materialized view to compute the average, min and max trip time between each taxi zone.
    From this MV, find the pair of taxi zones with the highest average trip time. You may need to use the dynamic filter pattern for this.

```sql
    CREATE MATERIALIZED VIEW taxi_zone_trip_stats AS
    SELECT
        pickup.zone AS pickup_zone,
        dropoff.zone AS dropoff_zone,
        AVG(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS avg_trip_time_minutes,
        MIN(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS min_trip_time_minutes,
        MAX(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS max_trip_time_minutes
    FROM
        trip_data td
    JOIN
        taxi_zone pickup ON td.PULocationID = pickup.location_id
    JOIN
        taxi_zone dropoff ON td.DOLocationID = dropoff.location_id
    GROUP BY
        pickup.zone, dropoff.zone;
```

Now from that created MV query the pair with highest average:
```sql
    SELECT pickup_zone, dropoff_zone, avg_trip_time_minutes
    FROM taxi_zone_trip_stats
    ORDER BY avg_trip_time_minutes DESC
    LIMIT 1;

```


**SOLUTION:**

Yorkville East, Steinway (avg_trip_time_minutes=1439.550000)





**`Bonus (no marks):`**
Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute, but the max trip time is 10 minutes and 20 minutes respectively.

>>> BLANK <<<

Q2) Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

```sql
    CREATE MATERIALIZED VIEW taxi_zone_trip_stats_with_count AS
    SELECT
        pickup.zone AS pickup_zone,
        dropoff.zone AS dropoff_zone,
        COUNT(*) AS number_of_trips,
        AVG(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS avg_trip_time_minutes,
        MIN(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS min_trip_time_minutes,
        MAX(EXTRACT(EPOCH FROM (td.tpep_dropoff_datetime - td.tpep_pickup_datetime)) / 60) AS max_trip_time_minutes
    FROM
        trip_data td
    JOIN
        taxi_zone pickup ON td.PULocationID = pickup.location_id
    JOIN
        taxi_zone dropoff ON td.DOLocationID = dropoff.location_id
    GROUP BY
        pickup.zone, dropoff.zone;
```

Query to get desired solution:

```sql
    SELECT pickup_zone, dropoff_zone, avg_trip_time_minutes, number_of_trips
    FROM taxi_zone_trip_stats_with_count
    ORDER BY avg_trip_time_minutes DESC
    LIMIT 1;
```

**SOLUTION:**

  pickup_zone   | dropoff_zone | avg_trip_time_minutes | number_of_trips
----------------+--------------+-----------------------+-----------------
 Yorkville East | Steinway     |           1439.550000 |               1

--> 1


Q3) From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?

First, find the latest pickup time in your trip_data table.

```sql
    SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time FROM trip_data;
```

Next, incorporate this into a query that filters records from the latest pickup time to 17 hours before.

```sql
    WITH LatestPickup AS (
    SELECT MAX(tpep_pickup_datetime) AS latest_pickup_time FROM trip_data
    ),
    FilteredTrips AS (
        SELECT
            td.PULocationID,
            COUNT(*) AS pickup_count
        FROM
            trip_data td, LatestPickup lp
        WHERE
            td.tpep_pickup_datetime BETWEEN (lp.latest_pickup_time - INTERVAL '17 HOURS') AND lp.latest_pickup_time
        GROUP BY
            td.PULocationID
    )
    SELECT
        tz.zone,
        ft.pickup_count
    FROM
        FilteredTrips ft
    JOIN
        taxi_zone tz ON ft.PULocationID = tz.location_id
    ORDER BY
        ft.pickup_count DESC
    LIMIT 3;
```

**SOLUTION:**

        zone         | pickup_count

---------------------+--------------

 LaGuardia Airport   |           19

 Lincoln Square East |           17

 JFK Airport         |           17

--> LaGuardia Airport, Lincoln Square East, JFK Airport

