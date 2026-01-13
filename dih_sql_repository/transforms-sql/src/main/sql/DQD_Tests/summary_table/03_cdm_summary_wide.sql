CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/03_cdm_summary_wide` AS


    WITH cdm_table AS (
        SELECT 
        main.cdm_name, 
        main.concept_name, 
        ROUND(AVG(count_concept),0) as mean_count,
        ROUND(PERCENTILE(count_concept, 0.25),0) AS q1_count,
        ROUND(PERCENTILE(count_concept, 0.50),0) AS median_count,
        ROUND(PERCENTILE(count_concept, 0.75),0) AS q3_count,
        ROUND(AVG(pct_concept),1) as mean_pct,
        ROUND(PERCENTILE(pct_concept, 0.25),1) AS q1_pct,
        ROUND(PERCENTILE(pct_concept, 0.50),1) AS median_pct,
        ROUND(PERCENTILE(pct_concept, 0.75),1) AS q3_pct

    FROM 
        `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/02_summary_long` as main
    GROUP BY 
        main.cdm_name, 
        main.concept_name
        ),

    all_table AS(
        SELECT 
        t.concept_name, 
        ROUND(AVG(t.count_concept),0) as allsites_mean_count,
        ROUND(PERCENTILE(t.count_concept, 0.25),0) AS allsites_q1_count,
        ROUND(PERCENTILE(t.count_concept, 0.50),0) AS allsites_median_count,
        ROUND(PERCENTILE(t.count_concept, 0.75),0) AS allsites_q3_count,
        ROUND(AVG(t.pct_concept),1) as allsites_mean_pct,
        ROUND(PERCENTILE(t.pct_concept, 0.25),1) AS allsites_q1_pct,
        ROUND(PERCENTILE(t.pct_concept, 0.50),1) AS allsites_median_pct,
        ROUND(PERCENTILE(t.pct_concept, 0.75),1) AS allsites_q3_pct
        
    FROM 
        `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/02_summary_long` as t
    GROUP BY 
        t.concept_name
    )

    SELECT
        t.concept_name, 
        c.cdm_name,
        c.mean_count,
        c.q1_count,
        c.median_count,
        c.q3_count,
        c.mean_pct,
        c.q1_pct,
        c.median_pct,
        c.q3_pct,
        t.allsites_mean_count,
        t.allsites_q1_count,
        t.allsites_median_count,
        t.allsites_q3_count,
        t.allsites_mean_pct,
        t.allsites_q1_pct,
        t.allsites_median_pct,
        t.allsites_q3_pct
    FROM all_table as t
    JOIN cdm_table as c
        ON t.concept_name = c.concept_name 