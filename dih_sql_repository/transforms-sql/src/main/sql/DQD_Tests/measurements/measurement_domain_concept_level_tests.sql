CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/measurement_domain_concept_level_tests` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_8, EXECUTOR_MEMORY_LARGE') AS

SELECT  A.*,
        UPPER(A.plausibleGender) as plausible_gender_upper,
        B.gender_concept_name,
        CASE
            WHEN A.value_as_number > A.plausibleValueHigh THEN 'TOO HIGH'
            WHEN A.value_as_number < A.plausibleValueLow THEN 'TOO LOW'
            WHEN A.value_as_number <= A.plausibleValueHigh 
              AND A.value_as_number >= A.plausibleValueLow THEN 'IN RANGE'
            ELSE 'NULL VALUE'
        END AS numeric_value_qc_check

FROM 

(
    SELECT * 
    FROM `/UNITE/LDS/clean/measurement` AS m
    INNER JOIN 
    -- thresholds defined by the DQD
    `/UNITE/[RP-4A9E27] DI&H - Data Quality/qc/DQD Thresholds/OMOP_CDMv5.3.1_Concept_Level` AS o
    ON m.measurement_concept_id = o.conceptId
    -- there are different thresholdholds depending on the units
    AND m.unit_concept_id = o.unitConceptId
    WHERE o.cdmTableName = 'MEASUREMENT'
) A

INNER JOIN

(
    SELECT person_id, gender_concept_name
    FROM `/UNITE/LDS/clean/person`
) B

ON A.person_id = B.person_id