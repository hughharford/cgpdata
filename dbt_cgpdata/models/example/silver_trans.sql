{{ config(materialized='view') }}

WITH imputed_data AS (
    SELECT
        timestamp,
        COALESCE(open, (high + low + close) / 3) AS open,
        COALESCE(high, (open + low + close) / 3) AS high,
        COALESCE(low, (open + high + close) / 3) AS low,
        COALESCE(close, (open + high + low) / 3) AS close,
        COALESCE(volume, 0) AS volume,
        ticker
    FROM {{ ref('decimal_trans') }}
)

SELECT * FROM imputed_data
