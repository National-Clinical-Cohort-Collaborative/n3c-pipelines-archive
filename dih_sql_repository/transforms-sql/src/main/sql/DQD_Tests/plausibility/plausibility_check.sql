CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/plausibility/plausibility_check` AS

SELECT A.*, B.gender_concept_id, B.gender_concept_name, B.location_id, B.year_of_birth, B.ethnicity_concept_id, B.race_concept_id

FROM

(   --Condition concepts associated with the female gender
    --Disease of the digestive system complicating pregnancy,4062790
    --Cyst of ovary, 197610
    --Noninflammatory vaginal disorders, 195867
    SELECT * FROM `/UNITE/LDS/clean/condition_occurrence`
    WHERE condition_concept_id IN (4062790, 197610, 195867)
) A

INNER JOIN

(   ---https://athena.ohdsi.org/search-terms/terms/8532 -- female
    SELECT person_id, gender_concept_id, gender_concept_name, location_id, year_of_birth, ethnicity_concept_id, race_concept_id, data_partner_id
    FROM `/UNITE/LDS/clean/person`
    WHERE gender_concept_id = 8507 ---identify all male with condition
) B

ON A.person_id = B.person_id
