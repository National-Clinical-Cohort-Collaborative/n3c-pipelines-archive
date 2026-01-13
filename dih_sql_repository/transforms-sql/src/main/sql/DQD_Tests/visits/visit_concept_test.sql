CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/visits/visit_concept_test` AS

WITH visit_counts AS
(
    SELECT data_partner_id, visit_concept_id, visit_concept_name, count(*) AS visit_count
    FROM `/UNITE/LDS/clean/visit_occurrence`
    GROUP BY data_partner_id, visit_concept_id, visit_concept_name
),

total_counts_by_site AS
(
    SELECT data_partner_id, count(*) AS total_site_visit_count
    FROM `/UNITE/LDS/clean/visit_occurrence`
    GROUP BY data_partner_id
)

SELECT  v.data_partner_id, 
        v.visit_concept_id, 
        v.visit_concept_name, 
        v.visit_count, 
        t.total_site_visit_count,
        ROUND((visit_count* 100 / total_site_visit_count),2) AS percent_visits

FROM visit_counts v
LEFT JOIN total_counts_by_site t
ON v.data_partner_id = t.data_partner_id

