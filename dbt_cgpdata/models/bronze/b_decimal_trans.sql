
-- Use the `ref` function to select from other models

{{ config(materialized='view') }}

WITH formatted_data AS (
    SELECT
        timestamp,
        ROUND(open, 6) AS open,
        ROUND(high, 6) AS high,
        ROUND(low, 6) AS low,
        ROUND(close, 6) AS close,
        volume,
        ticker
    FROM {{ ref('a_external_table_trans') }}
)

SELECT * FROM formatted_data
