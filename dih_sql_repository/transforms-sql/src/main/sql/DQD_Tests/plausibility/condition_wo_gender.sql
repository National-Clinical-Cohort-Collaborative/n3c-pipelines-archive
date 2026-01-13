CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/plausibility/condition_wo_gender` AS
--identifying condition occurrences that are missing or have other gender
WITH A AS(
    SELECT p.person_id, p.year_of_birth, p.data_partner_id, p.gender_concept_id, p.gender_concept_name, c.condition_occurrence_id, c.visit_occurrence_id, c.condition_concept_id, c.condition_concept_name
    FROM `ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239` p
    INNER JOIN `/UNITE/LDS/clean/condition_occurrence` c
    ON p.person_id = c.person_id AND p.data_partner_id = c.data_partner_id
    WHERE p.gender_concept_name IN ('No matching concept', 'UNKNOWN', 'Unknown', 'Gender unknown', 'OTHER') AND c.visit_occurrence_id IS NOT NULL
    GROUP BY p.person_id, p.year_of_birth, p.data_partner_id, p.gender_concept_id, p.gender_concept_name, c.condition_occurrence_id, c.visit_occurrence_id, c.condition_concept_id, c.condition_concept_name
    ORDER BY p.data_partner_id, p.gender_concept_name
),

B AS(
    SELECT data_partner_id,
    COUNT(*) AS num_missing_gender_by_site
    FROM A
    GROUP BY data_partner_id
), --row count of missing gender per site

C AS(
    SELECT data_partner_id,
    condition_concept_name,
    COUNT(*) AS num_missing_gender_by_site_and_type
    FROM A
    GROUP BY data_partner_id, condition_concept_name

) --row count of missing gender per site and type

SELECT  B.data_partner_id, 
        C.condition_concept_name,
        B.num_missing_gender_by_site,
        C.num_missing_gender_by_site_and_type,
        ROUND(C.num_missing_gender_by_site_and_type / B.num_missing_gender_by_site, 3) * 100 AS percent_condition_wo_gender
  
FROM C LEFT JOIN B
ON C.data_partner_id = B.data_partner_id