CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/plausibility/pre-eclampsia_age_less_than_12` AS

WITH I AS (
SELECT DISTINCT person_id, MIN(condition_start_date) as condition_start_date
FROM `/UNITE/LDS/clean/condition_occurrence`
WHERE condition_concept_id = 439393
GROUP BY person_id
),

J AS (SELECT p.data_partner_id, p.person_id, p.gender_concept_name, p.year_of_birth, I.condition_start_date
FROM `/UNITE/LDS/clean/person` p
INNER JOIN I ON p.person_id = I.person_id
),

K AS (
SELECT data_partner_id, person_id, gender_concept_name
FROM J
WHERE (year(condition_start_date) - year_of_birth) < 12
)
-- Dividing the number of relevant patients with that condition from that site by the total number of patients with that condition from that site, then multiply by 100
SELECT (count(distinct K.person_id) / count(distinct J.person_id)) * 100 as percent_implausible, count(distinct K.person_id) as person_count, K.data_partner_id, K.gender_concept_name
FROM K
INNER JOIN J on K.data_partner_id = J.data_partner_id
GROUP BY K.data_partner_id, K.gender_concept_name