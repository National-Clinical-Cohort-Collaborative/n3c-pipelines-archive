CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/conditions/conditions_domain_concept_level_tests` 
AS

SELECT  A.*,
        UPPER(A.plausibleGender) as plausible_gender_upper,
        B.gender_concept_name 
FROM

(
    SELECT * 
    FROM `/UNITE/LDS/clean/condition_occurrence` AS c
    INNER JOIN 
    `/UNITE/[RP-4A9E27] DI&H - Data Quality/qc/DQD Thresholds/OMOP_CDMv5.3.1_Concept_Level` AS o
    ON c.condition_concept_id = o.conceptId
    WHERE o.cdmTableName = 'CONDITION_OCCURRENCE'
) A

INNER JOIN

(
    SELECT person_id, gender_concept_name
    FROM `/UNITE/LDS/clean/person`
) B

ON A.person_id = B.person_id