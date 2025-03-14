{{ config(materialized='table') }}

WITH bronze_ready AS (
    SELECT
        *
    FROM {{ ref('c_silver_trans') }}
)

SELECT * FROM bronze_ready
