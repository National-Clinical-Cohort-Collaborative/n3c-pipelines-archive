CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/inpatient_admitted` AS

WITH A as
(
    SELECT visit_occurrence_id, person_id, data_partner_id, visit_concept_id
    FROM `/UNITE/LDS/clean/visit_occurrence`
    WHERE visit_concept_id IN (9201, 32037, 581379, 581383, 262, 8717)
),
--inpatient visit
B AS
(
    SELECT *
    FROM `/UNITE/LDS/clean/person`
)

SELECT A.*
FROM A INNER JOIN B ON A.person_id = B.person_id AND A.data_partner_id = B.data_partner_id