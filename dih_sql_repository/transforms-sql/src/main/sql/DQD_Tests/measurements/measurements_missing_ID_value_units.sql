CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/measurements_missing_ID_value_units` AS

-- FIND CASES WHERE we have a numeric result and no value for the unit
-- Some tests, like 'urine color' have a constant numeric value of 0.000
-- We detect these cases by couting the distinct number of numeric values

-- Added checks to catch missing id or value

SELECT      data_partner_id,
            unit_concept_id, 
            unit_source_value, 
            unit_concept_name, 
            measurement_concept_id, 
            measurement_concept_name,
            COUNT(*) AS row_count,
            COUNT(DISTINCT value_as_number) AS distinct_numeric_values

FROM `/UNITE/LDS/clean/measurement`

WHERE 

(
    measurement_concept_id = 0
)

OR

(
    value_as_number IS NULL
)
    
OR

(
    unit_concept_id = 0
    OR unit_concept_id IS NULL
)

GROUP BY    data_partner_id,
            unit_concept_id, 
            unit_source_value, 
            unit_concept_name, 
            measurement_concept_id, 
            measurement_concept_name