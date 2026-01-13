CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/04_cdm_summary_long` AS
    SELECT
        concept_name,
        AVG(CASE WHEN cdm_name = 'PCORNET' THEN mean_count END) AS PCORNET_mean_count,
        AVG(CASE WHEN cdm_name = 'PCORNET' THEN q1_count END) AS PCORNET_q1_count,
        AVG(CASE WHEN cdm_name = 'PCORNET' THEN median_count END) AS PCORNET_median_count,
        AVG(CASE WHEN cdm_name = 'PCORNET' THEN q3_count END) AS PCORNET_q3_count,
        AVG(CASE WHEN cdm_name = 'PCORNET' THEN mean_pct END) AS PCORNET_mean_pct,
        AVG(CASE WHEN cdm_name = 'PCORNET' THEN q1_pct END) AS PCORNET_q1_pct,
        AVG(CASE WHEN cdm_name = 'PCORNET' THEN median_pct END) AS PCORNET_median_pct,
        AVG(CASE WHEN cdm_name = 'PCORNET' THEN q3_pct END) AS PCORNET_q3_pct,
        AVG(CASE WHEN cdm_name = 'OMOP' THEN mean_count END) AS OMOP_mean_count,
        AVG(CASE WHEN cdm_name = 'OMOP' THEN q1_count END) AS OMOP_q1_count,
        AVG(CASE WHEN cdm_name = 'OMOP' THEN median_count END) AS OMOP_median_count,
        AVG(CASE WHEN cdm_name = 'OMOP' THEN q3_count END) AS OMOP_q3_count,
        AVG(CASE WHEN cdm_name = 'OMOP' THEN mean_pct END) AS OMOP_mean_pct,
        AVG(CASE WHEN cdm_name = 'OMOP' THEN q1_pct END) AS OMOP_q1_pct,
        AVG(CASE WHEN cdm_name = 'OMOP' THEN median_pct END) AS OMOP_median_pct,
        AVG(CASE WHEN cdm_name = 'OMOP' THEN q3_pct END) AS OMOP_q3_pct,
        AVG(CASE WHEN cdm_name = 'TRINETX' THEN mean_count END) AS TRINETX_mean_count,
        AVG(CASE WHEN cdm_name = 'TRINETX' THEN q1_count END) AS TRINETX_q1_count,
        AVG(CASE WHEN cdm_name = 'TRINETX' THEN median_count END) AS TRINETX_median_count,
        AVG(CASE WHEN cdm_name = 'TRINETX' THEN q3_count END) AS TRINETX_q3_count,
        AVG(CASE WHEN cdm_name = 'TRINETX' THEN mean_pct END) AS TRINETX_mean_pct,
        AVG(CASE WHEN cdm_name = 'TRINETX' THEN q1_pct END) AS TRINETX_q1_pct,
        AVG(CASE WHEN cdm_name = 'TRINETX' THEN median_pct END) AS TRINETX_median_pct,
        AVG(CASE WHEN cdm_name = 'TRINETX' THEN q3_pct END) AS TRINETX_q3_pct,
        AVG(CASE WHEN cdm_name = 'ACT' THEN mean_count END) AS ACT_mean_count,
        AVG(CASE WHEN cdm_name = 'ACT' THEN q1_count END) AS ACT_q1_count,
        AVG(CASE WHEN cdm_name = 'ACT' THEN median_count END) AS ACT_median_count,
        AVG(CASE WHEN cdm_name = 'ACT' THEN q3_count END) AS ACT_q3_count,
        AVG(CASE WHEN cdm_name = 'ACT' THEN mean_pct END) AS ACT_mean_pct,
        AVG(CASE WHEN cdm_name = 'ACT' THEN q1_pct END) AS ACT_q1_pct,
        AVG(CASE WHEN cdm_name = 'ACT' THEN median_pct END) AS ACT_median_pct,
        AVG(CASE WHEN cdm_name = 'ACT' THEN q3_pct END) AS ACT_q3_pct,
        AVG(allsites_mean_count) AS allsites_mean_count,
        AVG(allsites_q1_count) AS allsites_q1_count,
        AVG(allsites_median_count) AS allsites_median_count,
        AVG(allsites_q3_count) AS allsites_q3_count,
        AVG(allsites_mean_pct) AS allsites_mean_pct,
        AVG(allsites_q1_pct) AS allsites_q1_pct,
        AVG(allsites_median_pct) AS allsites_median_pct,
        AVG(allsites_q3_pct) AS allsites_q3_pct

    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/03_cdm_summary_wide`
    WHERE concept_name IS NOT NULL
    GROUP BY
        concept_name
    ORDER BY
        concept_name;
