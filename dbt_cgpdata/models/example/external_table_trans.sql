{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        timestamp_field_0 AS timestamp,
        CAST(_1__open AS FLOAT64) AS open,
        CAST(_2__high AS FLOAT64) AS high,
        CAST(_3__low AS FLOAT64) AS low,
        CAST(_4__close AS FLOAT64) AS close,
        CAST(_5__volume AS INT64) AS volume,
        'SPY' AS ticker
    FROM {{ source('raw_data', 'external_table') }}
)

SELECT * FROM source_data
