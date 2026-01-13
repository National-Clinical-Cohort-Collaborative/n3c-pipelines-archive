CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/unit_usage_by_measurement_concept` TBLPROPERTIES (foundry_transform_profile = 'NUM_EXECUTORS_8') AS
      
SELECT      
            data_partner_id,
            measurement_concept_id,
            measurement_concept_name,
            unit_source_value,
            unit_concept_id,
            unit_concept_name,
            COUNT(*) AS count_by_concept_and_site,
            PERCENTILE(value_as_number, 0.5) AS median_value

FROM `/UNITE/LDS/clean/measurement`

WHERE value_as_number IS NOT NULL

GROUP BY    measurement_concept_id,
            measurement_concept_name,
            data_partner_id,
            unit_source_value,
            unit_concept_id,
            unit_concept_name
