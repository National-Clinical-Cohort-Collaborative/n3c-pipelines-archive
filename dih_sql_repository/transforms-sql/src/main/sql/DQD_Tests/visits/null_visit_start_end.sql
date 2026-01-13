CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/visits/null_visit_start_end` AS
    
WITH missing AS
(
    SELECT      data_partner_id,
                visit_concept_name,
                COUNT(*) AS missing_row_count,
                "null start date" AS problem
    FROM `/UNITE/LDS/clean/visit_occurrence`
    WHERE visit_start_date IS NULL
    GROUP BY    data_partner_id,
                visit_concept_name

    UNION

    SELECT      data_partner_id,
                visit_concept_name,
                COUNT(*) AS missing_row_count,
                "null end date" AS problem
            
    FROM `/UNITE/LDS/clean/visit_occurrence`
    WHERE visit_end_date IS NULL
    GROUP BY    data_partner_id,
                visit_concept_name
),

totals AS
(
    SELECT      data_partner_id,
                visit_concept_name,
                COUNT(*) AS total_row_count
    FROM `/UNITE/LDS/clean/visit_occurrence`
    GROUP BY data_partner_id,
                visit_concept_name
)

SELECT  t.data_partner_id,
        t.visit_concept_name,
        problem,
        ROUND((missing_row_count / total_row_count), 4) * 100 AS percent_missing
FROM
totals t 
LEFT JOIN
missing m
ON m.data_partner_id = t.data_partner_id
AND m.visit_concept_name = t.visit_concept_name




    