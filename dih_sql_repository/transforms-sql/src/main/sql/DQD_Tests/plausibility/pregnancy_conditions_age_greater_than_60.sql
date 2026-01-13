CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/plausibility/pregnancy_conditions_age_greater_than_60` AS

WITH I AS (
SELECT *
FROM `/UNITE/LDS/clean/condition_occurrence` c
INNER JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/plausibility/maternal_health_concept_ids` p ON c.condition_concept_id = p.concept_id
),

J AS (
SELECT DISTINCT person_id, MAX(condition_start_date) as condition_start_date, condition_concept_id, condition_concept_name
FROM I
GROUP BY person_id, condition_concept_id, condition_concept_name
),

K AS (
SELECT p.data_partner_id, p.person_id, p.gender_concept_name, p.year_of_birth, u.condition_start_date, u.condition_concept_id, u.condition_concept_name
FROM `/UNITE/LDS/clean/person` p
INNER JOIN J u ON p.person_id = u.person_id
),

L AS (
SELECT data_partner_id, person_id, gender_concept_name, condition_concept_id, condition_concept_name
FROM K
WHERE (year(condition_start_date) - year_of_birth) >= 60
)
-- Dividing the number of relevant patients with that condition from that site by the total number of patients with that condition from that site, then multiply by 100
SELECT (count(distinct L.person_id) / count(distinct K.person_id)) * 100 as percent_implausible, count(distinct L.person_id) as person_count, L.data_partner_id, L.gender_concept_name, L.condition_concept_id, L.condition_concept_name
FROM L
INNER JOIN K on K.data_partner_id = L.data_partner_id
group by L.data_partner_id, L.gender_concept_name, L.condition_concept_id, L.condition_concept_name
