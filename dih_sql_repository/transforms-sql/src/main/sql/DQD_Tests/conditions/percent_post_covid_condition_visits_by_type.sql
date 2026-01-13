CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/conditions/percent_post_covid_condition_visits_by_type` AS

-- OF ALL POST COVID CONDITION VISITS AT A SITE, WHAT IS THE BREAKDOWN OF VISIT TYPE


-- SELECT visits associated with a POST COVID CONDITION diagnosis (UO9.9)
WITH  A AS
(
    SELECT  C.data_partner_id,
            V.visit_concept_name

    FROM `/UNITE/LDS/clean/condition_occurrence` C
    LEFT JOIN `/UNITE/LDS/clean/visit_occurrence`  V
    ON C.visit_occurrence_id = V.visit_occurrence_id  
    -- Disease caused by 2019-nCoV
    WHERE C.condition_concept_id = 705076
    AND C.visit_occurrence_id IS NOT NULL 
), 

B AS
(
    SELECT  data_partner_id,
            COUNT(*) AS num_post_covid_visits_by_site
    FROM A
    GROUP BY data_partner_id
),

C AS
(
    SELECT  data_partner_id,
            visit_concept_name,
            COUNT(*) AS num_post_covid_visits_by_site_and_type
    FROM A
    GROUP BY data_partner_id, visit_concept_name

)

SELECT  B.data_partner_id,
        C.visit_concept_name,
        ROUND(C.num_post_covid_visits_by_site_and_type / B.num_post_covid_visits_by_site, 3) * 100 AS percent_post_covid_visits

FROM C LEFT JOIN B
ON C.data_partner_id = B.data_partner_id