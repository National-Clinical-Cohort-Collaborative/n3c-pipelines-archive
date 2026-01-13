CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide` AS
    SELECT  
            p.data_partner_id, -- site
            REPLACE(m.cdm_name, 'OMOP (PEDSNET)', 'OMOP') as cdm_name, -- cdm

            -- CHARACTERISTICS
            p.number_of_people, -- Total N
        --    g.gender_concept_name, -- gender 
        --    g.num_people as gender_count_people, -- number of people by gender
        --    ROUND(g.percent_people,1) as gender_pct_people,
            e.ethnicity_concept_name, 
            e.count_people AS ethnicity_count_people,
            ROUND(e.percent_people,1) AS ethnicity_pct_people,
            ru.rurality, -- rurality
            ru.number_of_people AS rurality_count,
            ROUND(ru.percent_people,1) AS rurality_pct_people,
            vi.total_site_visit_count, -- Total visits/records
            vi.visit_type, -- visit type
            vi.visit_count, -- counts by visit type
            ROUND(vi.percent_visits, 1) as percent_visits,

            -- COVID TESTS
            c.TestGroup AS covid_test_group,
            c.value_as_concept_name AS covid_test_result,
            c.total_rows AS covid_test_count,
            ROUND(c.percent_of_testgroup,1) AS covid_test_percentbytype,
            ROUND(c.percent_of_total,1) AS covid_test_percentbysite,

            -- VITALS
            v.count_persons_anyvitals,
            v.percent_persons_anyvitals,
            v.count_persons_bp,
            v.percent_persons_bp,
            v.count_persons_bw,
            v.percent_persons_bw,

            -- COMPLETENESS/MISSINGNESS
            mdob.birthyear_quality, -- valid/missing dob
            mdob.count_birthyear AS birthyear_count, -- count of dob,
            ROUND(mdob.percent_people,1) AS birthyear_pct_people                    

            -- JOINS
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site` AS p -- person counts 
    LEFT JOIN `/UNITE/LDS/clean/manifest_clean` AS m -- manifest
        ON p.data_partner_id = m.data_partner_id
    --LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/count_by_gender` AS g -- gender 
    --    ON p.data_partner_id = g.data_partner_id
    LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/ethnicity_count` as e -- ethnicity 
        ON p.data_partner_id = e.data_partner_id
    LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/rurality` as ru -- rurality 
        ON p.data_partner_id = ru.data_partner_id
    LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/visits/visit_type` as vi -- visit counts disaggregated
        ON p.data_partner_id = vi.data_partner_id
    LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/missingness/birthyears` as mdob -- missing birth year
        ON p.data_partner_id = mdob.data_partner_id
    LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/covid_test_presence_value_mapped_fix` as c
        ON p.data_partner_id = c.data_partner_id
    LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/measurements_anyvitals` as v
        ON p.data_partner_id = v.data_partner_id

            -- GROUPS
    GROUP BY p.data_partner_id, -- site
            m.cdm_name, -- cdm
            p.number_of_people, -- Total N
        --    g.gender_concept_name, -- gender 
        --    g.num_people, -- number of people by gender
        --    g.percent_people, -- corrected line
            e.ethnicity_concept_name, 
            e.count_people,
            e.percent_people,
            ru.rurality, -- rurality
            ru.number_of_people,
            ru.percent_people,
            vi.total_site_visit_count,
            vi.visit_type, -- visit type
            vi.visit_count, -- counts by visit type
            vi.percent_visits,
            mdob.birthyear_quality, -- valid/missing dob
            mdob.count_birthyear,
            mdob.percent_people, -- corrected line
            birthyear_count, -- count of dob,
            birthyear_pct_people,
            c.TestGroup,
            c.value_as_concept_name,
            c.total_rows,
            c.percent_of_testgroup,
            c.percent_of_total,
            v.count_persons_anyvitals,
            v.percent_persons_anyvitals,
            v.count_persons_bp,
            v.percent_persons_bp,
            v.count_persons_bw,
            v.percent_persons_bw
            
    ORDER BY p.data_partner_id, ethnicity_concept_name
