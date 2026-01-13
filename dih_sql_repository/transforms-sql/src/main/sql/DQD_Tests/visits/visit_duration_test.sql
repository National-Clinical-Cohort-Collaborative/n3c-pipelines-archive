CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/visits/vists_duration_test` AS


SELECT  A.*, 
        B.total_visits_per_site,
        ROUND(A.total_visits / B.total_visits_per_site,4) * 100 AS percent_visits
        
FROM

(
        SELECT  visit_concept_id, 
                visit_concept_name, 
                data_partner_id,
                ROUND(AVG(visit_length),2) AS mean_visit_length,
                ROUND(STDDEV(visit_length),2) AS std_dev_visit_length,
                MIN(visit_length) AS min_visit_length,
                PERCENTILE(visit_length, 0.25) AS first_quartile_visit,
                PERCENTILE(visit_length, 0.5) AS median_visit,
                PERCENTILE(visit_length, 0.75) AS third_quartile_visit,
                MAX(visit_length) AS max_visit_length,
                COUNT(*) AS total_visits
        

        FROM

        (

        SELECT  visit_concept_id, 
                visit_concept_name, 
                DATEDIFF(visit_end_date, visit_start_date) AS visit_length,
                data_partner_id

        FROM `/UNITE/LDS/clean/visit_occurrence`

        )

        -- We want to remove negative visits
        -- we will report the stats for visits longer or equal to 0 days, and separately count the number less than 0
        WHERE visit_length >= 0
        GROUP BY visit_concept_id, visit_concept_name, data_partner_id
) A

LEFT JOIN

(
        SELECT  data_partner_id,
                COUNT(*) as total_visits_per_site

        FROM `/UNITE/LDS/clean/visit_occurrence`
        GROUP BY data_partner_id

) B

ON A.data_partner_id = B.data_partner_id