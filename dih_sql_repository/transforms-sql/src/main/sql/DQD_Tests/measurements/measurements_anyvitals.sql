CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/summary_table/measurements_anyvitals` AS
WITH total_persons AS (
    SELECT 
        data_partner_id, 
        COUNT(DISTINCT person_id) AS total_persons 
    FROM 
        `/UNITE/LDS/clean/person` 
    GROUP BY 
        data_partner_id
),
count_persons_anyvitals AS (
    SELECT 
        data_partner_id, 
        COUNT(DISTINCT CASE WHEN measurement_concept_id IN (3004249, 4154790, 4236281, 4248524, 3034703, 4268883, 3019962, 3012888, 36304130, 3013940, 4099154, 3013762, 3025315) THEN person_id ELSE NULL END) AS count_persons_anyvitals 
    FROM 
        `ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac` 
    GROUP BY 
        data_partner_id
),
count_persons_bp AS (
    SELECT 
        data_partner_id, 
        COUNT(DISTINCT CASE WHEN measurement_concept_id IN (3004249, 4154790, 4236281, 4248524, 3034703, 4268883, 3019962, 3012888, 36304130, 3013940) THEN person_id ELSE NULL END) AS count_persons_bp 
    FROM 
        `ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac` 
    GROUP BY 
        data_partner_id
),
count_persons_bw AS (
    SELECT 
        data_partner_id, 
        COUNT(DISTINCT CASE WHEN measurement_concept_id IN (4099154, 3013762, 3025315) THEN person_id ELSE NULL END) AS count_persons_bw 
    FROM 
        `ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac` 
    GROUP BY 
        data_partner_id
)

SELECT 
    p.data_partner_id,
    total_persons,
    IFNULL(m1.count_persons_anyvitals, 0) AS count_persons_anyvitals,
    ROUND(100.0 * IFNULL(m1.count_persons_anyvitals, 0) / total_persons, 1) AS percent_persons_anyvitals,
    IFNULL(m2.count_persons_bp, 0) AS count_persons_bp,
    ROUND(100.0 * IFNULL(m2.count_persons_bp, 0) / total_persons, 1) AS percent_persons_bp,
    IFNULL(m3.count_persons_bw, 0) AS count_persons_bw,
    ROUND(100.0 * IFNULL(m3.count_persons_bw, 0) / total_persons, 1) AS percent_persons_bw
FROM 
    total_persons AS p
LEFT JOIN 
    count_persons_anyvitals AS  m1
ON 
    p.data_partner_id = m1.data_partner_id
LEFT JOIN 
    count_persons_bp AS m2
ON 
    p.data_partner_id = m2.data_partner_id
LEFT JOIN 
    count_persons_bw AS m3
    ON 
    p.data_partner_id = m3.data_partner_id