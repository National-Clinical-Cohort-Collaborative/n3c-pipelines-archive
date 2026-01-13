CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/visits/visits_with_negative_duration_by_site_and_type` AS


SELECT  data_partner_id,
        visit_occurrence_id,
        visit_concept_id, 
        visit_concept_name, 
        DATEDIFF(visit_end_date, visit_start_date) AS visit_length,
        visit_start_date,
        visit_end_date
        

FROM `/UNITE/LDS/clean/visit_occurrence`
WHERE DATEDIFF(visit_end_date, visit_start_date) < 0