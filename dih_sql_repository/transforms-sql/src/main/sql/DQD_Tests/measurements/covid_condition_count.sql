CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/covid_condition_count` TBLPROPERTIES (foundry_transform_profiles = 'SHUFFLE_PARTITIONS_LARGE, EXECUTOR_MEMORY_MEDIUM') AS
    
    
    -- Finds the percent of people at each site who have COVID-19 diagnoses visits, broken down by visit type.


    WITH total_people_per_site AS
    (
        SELECT data_partner_id, 
        COUNT (DISTINCT person_id) AS total_people

        FROM `/UNITE/LDS/clean/person`
        GROUP BY data_partner_id
    ),


    covid_visits_by_type_and_site AS

    (
        SELECT  C.data_partner_id,
                V.visit_concept_name,
                COUNT(DISTINCT V.person_id) AS num_people_with_that_visit

        FROM `/UNITE/LDS/clean/condition_occurrence` C
        LEFT JOIN `/UNITE/LDS/clean/visit_occurrence`  V
        ON C.visit_occurrence_id = V.visit_occurrence_id
        -- Disease caused by 2019-nCoV
        WHERE C.condition_concept_id = 37311061
        GROUP BY C.data_partner_id, V.visit_concept_name

    )

    SELECT  C.*,
            P.total_people,
            ROUND((C.num_people_with_that_visit / P.total_people) * 100, 2) AS percent_people

    FROM covid_visits_by_type_and_site C
    LEFT JOIN total_people_per_site P
    ON C.data_partner_id = P.data_partner_id



    -- join visit
    -- percent patients with each visit type tagged with this diagnosis