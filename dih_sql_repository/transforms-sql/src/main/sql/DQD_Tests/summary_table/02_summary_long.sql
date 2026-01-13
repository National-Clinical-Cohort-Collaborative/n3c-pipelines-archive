CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/02_summary_long` AS

    SELECT  
            data_partner_id, -- site
            cdm_name, -- cdm
            concept_name, -- concept
            count_concept,
            pct_concept
    FROM
        (
            SELECT data_partner_id, 
                    cdm_name, 
                    'TOTAL_PEOPLE' AS concept_name, 
                    number_of_people as count_concept, 
                    NULL AS pct_concept
            FROM (
                SELECT DISTINCT data_partner_id, 
                                cdm_name, 
                                number_of_people
            FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide`)
            GROUP BY data_partner_id, cdm_name, number_of_people
            
            UNION
/*
            SELECT data_partner_id, 
                    cdm_name, 
                    CONCAT('GENDER_', gender_concept_name) AS concept_name, 
                    gender_count_people as count_concept,
                    gender_pct_people as pct_concept
            FROM (
                SELECT DISTINCT data_partner_id, 
                                cdm_name, 
                                gender_concept_name, 
                                gender_count_people,
                                gender_pct_people
            FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide`)
            GROUP BY data_partner_id, cdm_name, gender_concept_name, gender_count_people, gender_pct_people

            UNION
*/

            SELECT data_partner_id, 
                    cdm_name, 
                    CONCAT('ETHNICITY_',ethnicity_concept_name) AS concept_name, 
                    ethnicity_count_people as count_concept,
                    ethnicity_pct_people as pct_concept
            FROM (
                SELECT DISTINCT data_partner_id, 
                                cdm_name, 
                                ethnicity_concept_name, 
                                ethnicity_count_people,
                                ethnicity_pct_people
                FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide`
                )
            GROUP BY data_partner_id, cdm_name, ethnicity_concept_name, ethnicity_count_people, ethnicity_pct_people

            UNION

            SELECT 
                data_partner_id, 
                cdm_name, 
                CONCAT('RURALITY_', rurality) AS concept_name, 
                rurality_count as count_concept,
                rurality_pct_people as pct_concept
            FROM (
                SELECT DISTINCT 
                    data_partner_id, 
                    cdm_name, 
                    rurality, 
                    rurality_count,
                    rurality_pct_people
                FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide`
            ) AS rural_table
            GROUP BY 
                data_partner_id, 
                cdm_name, 
                CONCAT('RURALITY_', rurality),
                rurality_count,
                rurality_pct_people

            UNION

            SELECT 
                data_partner_id, 
                cdm_name, 
                CONCAT('VISIT_COUNT') AS concept_name, 
                total_site_visit_count as count_concept,
                NULL AS pct_concept
            FROM (
                SELECT DISTINCT 
                    data_partner_id, 
                    cdm_name, 
                    total_site_visit_count
                FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide`
            ) AS visit_table
            GROUP BY 
                data_partner_id, 
                cdm_name, 
                CONCAT('VISIT_COUNT'),
                total_site_visit_count
                
            UNION

            SELECT 
                data_partner_id, 
                cdm_name, 
                CONCAT('VISIT_TYPE_', visit_type) AS concept_name, 
                visit_count as count_concept,
                percent_visits as pct_concept
            FROM (
                SELECT DISTINCT 
                    data_partner_id, 
                    cdm_name, 
                    visit_type, 
                    visit_count,
                    percent_visits
                FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide`
            ) AS visittype_table
            GROUP BY 
                data_partner_id, 
                cdm_name, 
                visit_type,
                visit_count,
                percent_visits
                
            UNION 
            
            SELECT 
                data_partner_id, 
                cdm_name, 
                CONCAT('BIRTHYEAR_QUAL_', birthyear_quality) AS concept_name, 
                birthyear_count as count_concept,
                birthyear_pct_people as pct_concept
            FROM (
                SELECT DISTINCT 
                    data_partner_id, 
                    cdm_name, 
                    birthyear_quality, 
                    birthyear_count,
                    birthyear_pct_people
                FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide`
            ) AS birthyear_table
            GROUP BY 
                data_partner_id, 
                cdm_name, 
                CONCAT('BIRTHYEAR_QUAL_', birthyear_quality),
                birthyear_count,
                birthyear_pct_people
            
            UNION 
            
            SELECT
            data_partner_id, 
            cdm_name, 
            CONCAT('COVID_TEST (',covid_test_group,')_', covid_test_result) AS concept_name,
            covid_test_count,
            covid_test_percentbysite
            FROM (
                SELECT DISTINCT 
                    data_partner_id, 
                    cdm_name, 
                    covid_test_group,
                    covid_test_result,
                    covid_test_count,
                    covid_test_percentbytype,
                    covid_test_percentbysite
                     FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide`
            ) AS covid_table
            GROUP BY 
                data_partner_id, 
                cdm_name, 
                CONCAT('COVID_TEST (',covid_test_group,')_', covid_test_result),
                covid_test_count,
                covid_test_percentbysite
            
            UNION

            SELECT
            data_partner_id,
            cdm_name,
            CONCAT('VITALS_At least 1 BP or BW measurement') AS concept_name, 
            count_persons_anyvitals,
            percent_persons_anyvitals
            FROM (
                SELECT DISTINCT 
                    data_partner_id, 
                    cdm_name, 
                    count_persons_anyvitals,
                    percent_persons_anyvitals
                     FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide`
            ) AS vital_table1
            GROUP BY 
                data_partner_id, 
                cdm_name, 
                CONCAT('VITALS_Any Vitals'), 
                count_persons_anyvitals,
                percent_persons_anyvitals
            
            UNION

            SELECT
            data_partner_id,
            cdm_name,
            CONCAT('VITALS_Blood Pressure') AS concept_name,
            count_persons_bp,
            percent_persons_bp
            FROM (
                SELECT DISTINCT 
                    data_partner_id, 
                    cdm_name, 
                    count_persons_bp,
                    percent_persons_bp
                     FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide`
            ) AS vital_table2
            GROUP BY 
                data_partner_id, 
                cdm_name, 
                CONCAT('VITALS_Blood Pressure'),
                count_persons_bp,
                percent_persons_bp

            UNION

            SELECT
            data_partner_id,
            cdm_name,
            CONCAT('VITALS_Body Weight') AS concept_name,
            count_persons_bw,
            percent_persons_bw
            FROM (
                SELECT DISTINCT 
                    data_partner_id, 
                    cdm_name, 
                    count_persons_bw,
                    percent_persons_bw
                     FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/01_summary_wide`
            ) AS vital_table3
            GROUP BY 
                data_partner_id, 
                cdm_name, 
                CONCAT('VITALS_Body Weight'),
                count_persons_bw,
                percent_persons_bw

    ORDER BY data_partner_id

)