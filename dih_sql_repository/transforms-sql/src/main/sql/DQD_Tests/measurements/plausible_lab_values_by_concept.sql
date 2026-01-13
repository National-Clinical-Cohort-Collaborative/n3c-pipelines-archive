CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/plausible_lab_values_by_concept` AS
             

SELECT      
    A.measurement_concept_id, 
    A.measurement_concept_name, 
    A.measurement_status,
    A.data_partner_id, 
    A.number_rows,
    A.distinct_patients,
    A.plausibleValueHighThreshold, 
    A.plausibleValueLowThreshold,
    B.mean_result_value,
    B.stdev_result_value,
    B.total_distinct_patients,
    B.total_rows,

    -- percentage of total rows having this status
    100 * ROUND((A.number_rows / B.total_rows), 3) AS percent_rows,
    
    
    -- percentage of patients having at least one row in this status
    100 * ROUND((A.distinct_patients / B.total_distinct_patients), 3) AS percent_patients

    
    FROM

    (
        SELECT  measurement_concept_id, 
                measurement_concept_name, 
                measurement_status, 
                data_partner_id,
                COUNT(*) AS number_rows,
                COUNT (DISTINCT person_id) AS distinct_patients,
                plausibleValueHighThreshold,
                plausibleValueLowThreshold


        FROM

        (
            SELECT *,
                CASE 
                    WHEN value_as_number < plausibleValueLow  THEN 'TOO LOW'
                    WHEN value_as_number > plausibleValueHigh THEN 'TOO HIGH'
                    ELSE 'IN RANGE'
                END AS measurement_status

            FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/measurement_domain_concept_level_tests`

            

        )

        GROUP BY measurement_concept_id, measurement_concept_name, measurement_status, data_partner_id, plausibleValueHighThreshold, plausibleValueLowThreshold
    ) A

    LEFT JOIN

    (   
        -- calculate the totals number of measurements for each concept at each site
        SELECT measurement_concept_id, 
            COUNT(*) AS total_rows,
            COUNT (DISTINCT person_id) AS total_distinct_patients,
            data_partner_id,
            AVG(value_as_number) AS mean_result_value,
            STD(value_as_number) AS stdev_result_value

            FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/measurement_domain_concept_level_tests`
            GROUP BY measurement_concept_id, measurement_concept_name,data_partner_id

    ) B

    ON A.measurement_concept_id = B.measurement_concept_id
    AND A.data_partner_id = B.data_partner_id
