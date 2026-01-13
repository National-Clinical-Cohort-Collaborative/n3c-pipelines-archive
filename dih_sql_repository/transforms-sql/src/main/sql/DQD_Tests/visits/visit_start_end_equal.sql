CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/visits/visit_start_end_equal` AS
    

WITH A AS
(
    SELECT  data_partner_id, 
            visit_concept_id,
            visit_concept_name,
            visit_occurrence_id,
            CASE
                WHEN visit_start_date = visit_end_date THEN 'end EQUALS start'
                WHEN visit_start_date > visit_end_date THEN 'end BEFORE start'
                WHEN visit_start_date < visit_end_date THEN 'end AFTER start'
                WHEN visit_start_date IS NULL AND visit_end_date IS NULL THEN 'BOTH dates null'
                WHEN visit_start_date IS NULL AND visit_end_date IS NOT NULL THEN 'null start date'
                WHEN visit_end_date IS NULL AND visit_start_date IS NOT NULL THEN 'null end date'
                
            END AS end_date_status
    FROM `/UNITE/LDS/clean/visit_occurrence`
),


B AS
(
    SELECT  data_partner_id,
            visit_concept_id,
            visit_concept_name,
            end_date_status,
            COUNT(DISTINCT visit_occurrence_id) AS num_visits

    FROM A
    GROUP BY data_partner_id,
             visit_concept_id,
             visit_concept_name,
             end_date_status
),


C AS
(
    SELECT  data_partner_id,
            visit_concept_id,
            visit_concept_name,
            COUNT(DISTINCT visit_occurrence_id) AS total_visits_of_type

    FROM   `/UNITE/LDS/clean/visit_occurrence`
    GROUP BY    data_partner_id,
                visit_concept_id,
                visit_concept_name

) 

SELECT  B.*,
        C.total_visits_of_type,
        ROUND(B.num_visits / C.total_visits_of_type, 3) * 100 AS percent_visits
FROM B
LEFT JOIN C
ON B.data_partner_id = C.data_partner_id
AND B.visit_concept_id = C.visit_concept_id

