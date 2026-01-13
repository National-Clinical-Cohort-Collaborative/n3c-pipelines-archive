CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/visits/visit_U07.1_percent_test` AS

WITH A AS
(
    SELECT COUNT(c_person_id) AS visits_U07_1 
    FROM
    (
        SELECT c_person_id
        FROM
        (
            SELECT  person_id as c_person_id,
                    data_partner_id as c_data_partner_id,
                    visit_occurrence_id as c_visit_occurrence_id
            FROM `/UNITE/LDS/clean/condition_occurrence`
            WHERE condition_concept_id = 37311061
        )
        INNER JOIN 
        (
            SELECT  person_id as v_person_id,
                    data_partner_id as v_data_partner_id,
                    visit_occurrence_id as v_visit_occurrence_id
            FROM `/UNITE/LDS/clean/visit_occurrence` 
        )
        ON c_person_id = v_person_id 
        AND c_data_partner_id = v_data_partner_id 
        AND c_visit_occurrence_id = v_visit_occurrence_id
    )
) 
, 

B AS
(
    SELECT COUNT(visit_occurrence_id) AS total_visits
    FROM `/UNITE/LDS/clean/visit_occurrence`
)

SELECT ROUND((SELECT DISTINCT * FROM A) / (SELECT DISTINCT * FROM B), 4) * 100 AS percent_visits