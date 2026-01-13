CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/05_sites_cdm_summary` AS



    SELECT  s.data_partner_id, -- site
            s.cdm_name, -- cdm
            s.concept_name, -- concept
            s.count_concept,
            s.pct_concept,
            (CASE 
                WHEN s.cdm_name = 'PCORNET' THEN 
                    (CASE WHEN s.pct_concept < c.PCORNET_q1_pct THEN '1st Quartile'
                    WHEN s.pct_concept >= c.PCORNET_q1_pct AND s.pct_concept < c.PCORNET_median_pct THEN '2nd Quartile'
                    WHEN s.pct_concept >= c.PCORNET_median_pct AND s.pct_concept < c.PCORNET_q3_pct THEN '3rd Quartile'
                    WHEN s.pct_concept >= c.PCORNET_q3_pct THEN '4th Quartile' END) 
                WHEN s.cdm_name = 'OMOP' THEN 
                    (CASE WHEN s.pct_concept <= c.OMOP_q1_pct THEN '1st Quartile'
                    WHEN s.pct_concept > c.OMOP_q1_pct AND s.pct_concept <= c.OMOP_median_pct THEN '2nd Quartile'
                    WHEN s.pct_concept > c.OMOP_median_pct AND s.pct_concept <= c.OMOP_q3_pct THEN '3rd Quartile'
                    WHEN s.pct_concept > c.OMOP_q3_pct THEN '4th Quartile' END) 
                WHEN s.cdm_name = 'TRINETX' THEN 
                    (CASE WHEN s.pct_concept <= c.TRINETX_q1_pct THEN '1st Quartile'
                    WHEN s.pct_concept > c.TRINETX_q1_pct AND s.pct_concept <= c.TRINETX_median_pct THEN '2nd Quartile'
                    WHEN s.pct_concept > c.TRINETX_median_pct AND s.pct_concept <= c.TRINETX_q3_pct THEN '3rd Quartile'
                    WHEN s.pct_concept > c.TRINETX_q3_pct THEN '4th Quartile' END) 
                WHEN s.cdm_name = 'ACT' THEN 
                    (CASE WHEN s.pct_concept <= c.ACT_q1_pct THEN '1st Quartile'
                    WHEN s.pct_concept > c.ACT_q1_pct AND s.pct_concept <= c.ACT_median_pct THEN '2nd Quartile'
                    WHEN s.pct_concept > c.ACT_median_pct AND s.pct_concept <= c.ACT_q3_pct THEN '3rd Quartile'
                    WHEN s.pct_concept > c.ACT_q3_pct THEN '4th Quartile' END) 
                END 
            ) 
            AS site_quartile_within_cdm,
            c.PCORNET_mean_count,
            c.PCORNET_q1_count,
            c.PCORNET_median_count,
            c.PCORNET_q3_count,
            c.PCORNET_mean_pct,
            c.PCORNET_q1_pct,
            c.PCORNET_median_pct,
            c.PCORNET_q3_pct,
            c.OMOP_mean_count,
            c.OMOP_q1_count,
            c.OMOP_median_count,
            c.OMOP_q3_count,
            c.OMOP_mean_pct,
            c.OMOP_q1_pct,
            c.OMOP_median_pct,
            c.OMOP_q3_pct,
            c.TRINETX_mean_count,
            c.TRINETX_q1_count,
            c.TRINETX_median_count,
            c.TRINETX_q3_count,
            c.TRINETX_mean_pct,
            c.TRINETX_q1_pct,
            c.TRINETX_median_pct,
            c.TRINETX_q3_pct,
            c.ACT_mean_count,
            c.ACT_q1_count,
            c.ACT_median_count,
            c.ACT_q3_count,
            c.ACT_mean_pct,
            c.ACT_q1_pct,
            c.ACT_median_pct,
            c.ACT_q3_pct,
            c.allsites_mean_count,
            c.allsites_q1_count,
            c.allsites_median_count,
            c.allsites_q3_count,
            c.allsites_mean_pct,
            c.allsites_q1_pct,
            c.allsites_median_pct,
            c.allsites_q3_pct,
            (CASE WHEN s.pct_concept < c.allsites_q1_pct THEN '1st Quartile'
                    WHEN s.pct_concept >= c.allsites_q1_pct AND s.pct_concept < c.allsites_median_pct THEN '2nd Quartile'
                    WHEN s.pct_concept >= c.allsites_median_pct AND s.pct_concept < c.allsites_q3_pct THEN '3rd Quartile'
                    WHEN s.pct_concept >= c.allsites_q3_pct THEN '4th Quartile' END) AS site_quartile_allsites
            

    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/04_cdm_summary_long` AS c
    FULL OUTER JOIN   `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/02_summary_long` as s
    ON c.concept_name = s.concept_name
    ORDER BY
        data_partner_id,
        concept_name;
