{{
    config(
        materialized='view'
    )
}}

with tripdata as
(
  select *
  from {{ source('staging', 'fhv')}}
  where dispatching_base_num is not null
  and extract(year from pickup_datetime) = 2019
),
processed as (
    select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as trip_id,
    dispatching_base_num,
    cast(p_ulocation_id as integer) as pickup_location_id,
    cast(d_olocation_id as integer) as dropoff_location_id,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(drop_off_datetime as timestamp) as dropoff_datetime,
    {{ dbt.date_trunc("year", "pickup_datetime") }} as pickup_year,
    -- misc
    sr_flag,
    affiliated_base_number
    from tripdata
)
select processed.trip_id,
    processed.dispatching_base_num,
    processed.pickup_location_id,
    processed.dropoff_location_id,
    processed.pickup_datetime,
    processed.dropoff_datetime,
    processed.sr_flag,
    processed.affiliated_base_number
from processed
where processed.pickup_year = '2019-01-01 00:00:00 UTC'

-- dbt build --select <model_name> --vars '{ is_test_run: false }'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}