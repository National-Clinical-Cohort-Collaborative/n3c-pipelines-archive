CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/covid_test_presence_value_mapped_fix` AS

WITH recategorized_data AS
(
    SELECT 
        data_partner_id,
        TestGroup,
        measurement_concept_name,
        measurement_concept_id,
        num_rows,
        CASE 
            WHEN value_as_concept_name IN ('Positive', 'Detected', 'Presumptive positive', 'Reactive') THEN 'Positive'
            WHEN value_as_concept_name IN ('Negative', 'Non-Reactive', 'Not detected', 'None detected', 'Not detected/negative', 'Nonreactive', 'No reaction', 'Normal', 'Normal range') THEN 'Negative'
            --WHEN value_as_concept_name IN ('Performed') THEN 'Performed'
            --WHEN value_as_concept_name IN ('Not performed', 'Test not performed', 'Not done', 'Test not done') THEN 'Not performed'
            ELSE 'Missing/Null/Other'
        END as value_as_concept_name
    FROM 
        `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/measurements/covid_test_presence_value_mapped`
),

totals AS
(
    SELECT 
        data_partner_id,
        TestGroup,
        value_as_concept_name,
        SUM(num_rows) AS total_rows,
        SUM(SUM(num_rows)) OVER (PARTITION BY data_partner_id, TestGroup) AS total_rows_per_testgroup,
        SUM(SUM(num_rows)) OVER (PARTITION BY data_partner_id) AS total_rows_per_partner
    FROM 
        recategorized_data
    GROUP BY
        data_partner_id,
        TestGroup,
        value_as_concept_name
)

SELECT 
    data_partner_id,
    TestGroup,
    value_as_concept_name,
    total_rows,
    total_rows_per_testgroup,
    total_rows_per_partner,
    ROUND(total_rows * 100.0 / total_rows_per_testgroup, 2) AS percent_of_testgroup,
    ROUND(total_rows * 100.0 / total_rows_per_partner, 2) AS percent_of_total
FROM 
    totals
ORDER BY 
    data_partner_id, 
    TestGroup, 
    value_as_concept_name;
