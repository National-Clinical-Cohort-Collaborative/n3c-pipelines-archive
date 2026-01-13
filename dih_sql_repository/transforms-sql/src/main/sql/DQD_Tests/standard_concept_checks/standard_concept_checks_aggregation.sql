CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/standard_concept_checks_aggregated` AS

WITH non_standard_counts AS (
    SELECT domain, data_partner_id, count(*) as num_records, 'Non-zero concept' as type FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/standard_concept_checks`
    WHERE concept_id != 0 OR concept_id IS NULL
    GROUP BY domain, data_partner_id

    UNION ALL

    SELECT domain, data_partner_id, count(*) as num_records, 'Zero concept' as type FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/standard_concept_checks`
    WHERE concept_id == 0
    GROUP BY domain, data_partner_id
)

SELECT 
      non_standard_counts.*
    , row_counts.tot_num_records
    , (non_standard_counts.num_records/row_counts.tot_num_records)*100 as percentage
FROM non_standard_counts
JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/domain_row_counts` row_counts
ON non_standard_counts.domain = row_counts.domain
AND non_standard_counts.data_partner_id = row_counts.data_partner_id
