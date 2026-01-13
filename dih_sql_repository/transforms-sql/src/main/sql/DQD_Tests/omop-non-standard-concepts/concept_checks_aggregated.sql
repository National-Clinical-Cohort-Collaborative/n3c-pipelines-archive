CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/concept_checks_aggregated` AS

WITH non_standard as (
    SELECT domain
    , data_partner_id 
    , standard_concept
    , omop_domain_concept_id as concept_id
    , total_count 
    , distinct_person_count
    , concept_name
    , domain_id
    , vocabulary_id
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/non_standard_concepts_found_in_omop_domain`

), 
non_standard_counts AS (
    SELECT 
    domain
    , data_partner_id 
    , 'null concept, non-standard' as type 
    , sum(total_count) as num_records 
    FROM non_standard
    WHERE standard_concept is null
    group by domain, data_partner_id, type
    
    UNION

    SELECT domain
    , data_partner_id
    , 'standard concept' as type 
    , sum(total_count) as num_records
    FROM non_standard
    WHERE standard_concept = 'S'
    group by domain, data_partner_id, type

    UNION

     SELECT domain
    , data_partner_id
    , 'classification concept' as type 
    , sum(total_count) as num_records
    FROM non_standard
    WHERE standard_concept = 'C'
     group by domain, data_partner_id, type
)

SELECT 
      non_standard_counts.*
    , row_counts.tot_num_records
    , (non_standard_counts.num_records/row_counts.tot_num_records)*100 as percentage
FROM non_standard_counts
JOIN `ri.foundry.main.dataset.b3558700-d4b5-43d1-9e0e-05a49feeb717` row_counts
ON non_standard_counts.domain = row_counts.domain
AND non_standard_counts.data_partner_id = row_counts.data_partner_id