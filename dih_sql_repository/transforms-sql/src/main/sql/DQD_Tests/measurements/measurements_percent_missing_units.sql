CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/measurements_percent_missing_units` AS

WITH A as (
    SELECT  data_partner_id,
            COUNT(*) AS total_units
    FROM `/UNITE/LDS/clean/measurement`
    WHERE value_as_number IS NOT NULL
    GROUP BY data_partner_id
)
,
B as (
    SELECT  data_partner_id,
            SUM(row_count) AS missing_units
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/measurements_missing_units`
    GROUP BY data_partner_id

)

SELECT  A.data_partner_id AS data_partner_id,
        A.total_units AS total_units, 
        B.missing_units AS missing_units,
        CAST(B.missing_units AS DECIMAL) / A.total_units AS percent_missing
FROM A
INNER JOIN B ON A.data_partner_id = B.data_partner_id;