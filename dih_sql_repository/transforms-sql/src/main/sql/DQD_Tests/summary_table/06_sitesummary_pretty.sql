CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/06_sitesummary_pretty` AS
    SELECT 
        s.data_partner_id AS DataPartnerID, -- site
        s.cdm_name AS CDM, -- cdm
        CASE 
            WHEN concept_name LIKE 'TOTAL_PEOPLE%' THEN 'Total'
            WHEN concept_name LIKE 'VISIT_TYPE_%' THEN 'Visit Type'
            WHEN concept_name LIKE 'VISIT_COUNT' THEN 'Total'
            WHEN concept_name LIKE 'BIRTHYEAR_QUAL_%' THEN 'Birth Year Quality'
            WHEN concept_name LIKE 'ETHNICITY_%' THEN 'Ethnicity'
            WHEN concept_name LIKE 'RURALITY_%' THEN 'Rurality'
            WHEN concept_name LIKE 'COVID_TEST (Molecular (PCR & NAATs))%' THEN 'SARS-CoV-2 Test (Molecular - PCR & NAATs)' 
            WHEN concept_name LIKE 'COVID_TEST (Antigen)%' THEN 'SARS-CoV-2 Test (Antigen)' 
            WHEN concept_name LIKE 'VITALS_At least 1 BP or BW measurement%' THEN 'Vitals (Measurement)'
            WHEN concept_name LIKE 'COVID_TEST (Antibody)%' THEN 'SARS-CoV-2 Test (Antibody)' 
            WHEN concept_name LIKE 'VITALS_Blood Pressure%' THEN 'Vitals (Measurement)'
            WHEN concept_name LIKE 'VITALS_Body Weight' THEN 'Vitals (Measurement)'
        END AS Category,
        CASE
            WHEN concept_name LIKE 'VISIT_TYPE_ED' THEN 'ED'
            WHEN concept_name LIKE 'VISIT_TYPE_Inpatient' THEN 'Inpatient'
            WHEN concept_name LIKE 'VISIT_TYPE_No matching concept' THEN 'No matching concept'
            WHEN concept_name LIKE 'VISIT_TYPE_Outpatient/Ambulatory' THEN 'Outpatient/Ambulatory'           
            WHEN concept_name LIKE 'BIRTHYEAR_QUAL_Missing/Implausible DOB' THEN 'Missing/Implausible'
            WHEN concept_name LIKE 'BIRTHYEAR_QUAL_Valid DOB' THEN 'Valid'
            WHEN concept_name LIKE 'TOTAL_PEOPLE' THEN 'Persons'
            WHEN concept_name LIKE 'VISIT_COUNT' THEN 'Visits'
            WHEN concept_name LIKE 'COVID_TEST (Antibody)_Missing/Null/Other' THEN 'Missing/Null/Other'
            WHEN concept_name LIKE 'COVID_TEST (Antibody)_Negative' THEN 'Negative'
            WHEN concept_name LIKE 'COVID_TEST (Antibody)_Positive' THEN 'Positive'
            WHEN concept_name LIKE 'COVID_TEST (Antigen)_Missing/Null/Other' THEN 'Missing/Null/Other'
            WHEN concept_name LIKE 'COVID_TEST (Antigen)_Negative' THEN 'Negative'
            WHEN concept_name LIKE 'COVID_TEST (Antigen)_Positive' THEN 'Positive'
            WHEN concept_name LIKE 'COVID_TEST (Molecular (PCR & NAATs))_Missing/Null/Other' THEN 'Missing/Null/Other'
            WHEN concept_name LIKE 'COVID_TEST (Molecular (PCR & NAATs))_Negative' THEN 'Negative'
            WHEN concept_name LIKE 'COVID_TEST (Molecular (PCR & NAATs))_Positive' THEN 'Positive'
            WHEN concept_name LIKE '%\_%' THEN TRIM(SUBSTR(concept_name, INSTR(concept_name, '_') + 1))
            ELSE TRIM(concept_name)
        END AS Concept,
        CASE 
            WHEN s.pct_concept IS NULL THEN s.count_concept  
            ELSE CONCAT(s.pct_concept, '% (', s.count_concept, ')') 
            END AS SitePercentN,
        CASE
            WHEN s.site_quartile_within_cdm IS NULL THEN '-' 
            ELSE s.site_quartile_within_cdm 
            END AS RankWithinCDM,
        CASE 
            WHEN s.PCORNET_mean_pct IS NULL THEN '-'
            ELSE CONCAT(s.PCORNET_mean_pct, '%')
            END AS PCORNETmeanpct,
        CASE
            WHEN s.PCORNET_median_pct IS NULL THEN '-'
            ELSE CONCAT(s.PCORNET_median_pct, '% (', s.PCORNET_q1_pct, '-', s.PCORNET_q3_pct, '%)') 
            END AS PCORNETMedianIQR,
        CASE 
            WHEN s.OMOP_mean_pct IS NULL THEN '-'
            ELSE CONCAT(s.OMOP_mean_pct, '%')
            END AS OMOPmeanpct,
        CASE
            WHEN s.OMOP_median_pct IS NULL THEN '-'
            ELSE CONCAT(s.OMOP_median_pct, '% (', s.OMOP_q1_pct, '-', s.OMOP_q3_pct, '%)') 
            END AS OMOPMedianIQR,
        CASE 
            WHEN s.TRINETX_mean_pct IS NULL THEN '-'
            ELSE CONCAT(s.TRINETX_mean_pct, '%')
            END AS TRINETXmeanpct,
        CASE
            WHEN s.TRINETX_median_pct IS NULL THEN '-'
            ELSE CONCAT(s.TRINETX_median_pct, '% (', s.TRINETX_q1_pct, '-', s.TRINETX_q3_pct, '%)') 
            END AS TRINETXMedianIQR,
        CASE 
            WHEN s.ACT_mean_pct IS NULL THEN '-'
            ELSE CONCAT(s.ACT_mean_pct, '%')
            END AS ACTmeanpct,
        CASE
            WHEN s.ACT_median_pct IS NULL THEN '-'
            ELSE CONCAT(s.ACT_median_pct, '% (', s.ACT_q1_pct, '-', s.ACT_q3_pct, '%)') 
            END AS ACTMedianIQR,
        CASE
            WHEN s.allsites_median_pct IS NULL THEN '-'
            ELSE CONCAT(s.allsites_median_pct, '% (', s.allsites_q1_pct, '-', s.allsites_q3_pct, '%)') 
            END AS AllSitesMedianIQR,
        CASE
            WHEN s.site_quartile_allsites IS NULL THEN '-' 
            ELSE s.site_quartile_allsites 
            END AS RankWithinAllSites 
        
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/05_sites_cdm_summary` as s
    WHERE concept_name IS NOT NULL
    ORDER BY
        data_partner_id,
        CASE WHEN Category = 'Total' AND Concept = 'Persons' THEN 1
           -- WHEN Category = 'Gender' THEN 2
            WHEN Category = 'Ethnicity' AND Concept = 'Hispanic or Latino' THEN 3
            WHEN Category = 'Ethnicity' AND Concept = 'Not Hispanic or Latino' THEN 3
            WHEN Category = 'Ethnicity' AND Concept = 'Missing/Unknown' THEN 4
            WHEN Category = 'Rurality' AND Concept = 'Urban' THEN 5
            WHEN Category = 'Rurality' AND Concept = 'Rural' THEN 6
            WHEN Category = 'Rurality' AND Concept = 'Missing' THEN 7
            WHEN Category = 'Birth Year Quality' AND Concept = 'Valid' THEN 8
            WHEN Category = 'Birth Year Quality' AND Concept = 'Missing/Implausible' THEN 9
            WHEN Category = 'Total' AND Concept = 'Visits' THEN 10
            WHEN Category = 'Visit Type' AND Concept = 'Inpatient' THEN 11
            WHEN Category = 'Visit Type' AND Concept = 'Outpatient/Ambulatory' THEN 12            
            WHEN Category = 'Visit Type' AND Concept = 'ED' THEN 13
            WHEN Category = 'Visit Type' AND Concept = 'No matching concept' THEN 14            
            ELSE 15 END