CREATE TABLE `ri.foundry.main.dataset.3128064a-eb32-4a1d-acb4-647eb7b10608` AS
    
   
WITH rescued as (
    SELECT 
      omop_source_domain as domain
    , data_partner_id 
    , target_standard_concept as standard_concept
--    , source_standard_concept as standard_concept
    , domain_concept_id as concept_id
    , total_count 
    , distinct_person_count
    , concept_name
--    , source_concept_domain_id as domain_id
    , target_domain_id as domain_id
    , target_vocabulary_id
    FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/re_map_non_standard_concepts_found_in_omop_domain`

), 
rescued_counts AS (
    SELECT 
    domain
    , data_partner_id 
    , 'null concept, non-standard' as type 
    , sum(total_count) as num_records 
    FROM rescued
    WHERE standard_concept is null
    group by domain, data_partner_id, type
    
    UNION

    SELECT domain
    , data_partner_id
    , 'standard concept' as type 
    , sum(total_count) as num_records
    FROM rescued
    WHERE standard_concept = 'S'
    group by domain, data_partner_id, type

    UNION

     SELECT domain
    , data_partner_id
    , 'classification concept' as type 
    , sum(total_count) as num_records
    FROM rescued
    WHERE standard_concept = 'C'
     group by domain, data_partner_id, type
),temp as (
    select omop_source_domain,data_partner_id, sum(total_count) as num_records,sum(total_count)/2 as fix_num 
    from `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/re_map_non_standard_concepts_found_in_omop_domain`
    where concept_id in (
        SELECT concept_id_1
        FROM `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71`
        where relationship_id ='Maps to'
        GROUP BY concept_id_1
        HAVING COUNT(*) > 1
    )
    group by omop_source_domain,data_partner_id

),
last_step as(
SELECT 
      rescued_counts.*
    , row_counts.tot_num_records
    , (rescued_counts.num_records/row_counts.tot_num_records)*100 as percentage
FROM rescued_counts
JOIN `ri.foundry.main.dataset.b3558700-d4b5-43d1-9e0e-05a49feeb717` row_counts
ON rescued_counts.domain = row_counts.domain
AND rescued_counts.data_partner_id = row_counts.data_partner_id
)


select *,(num_records/new_tot_num_records)*100 as new_percentage from (
    SELECT re.*,t.num_records as new_num_records, t.fix_num,(case when type='standard concept' then tot_num_records +fix_num else tot_num_records end) as new_tot_num_records
    FROM last_step re left join temp t on re.domain=t.omop_source_domain and re.data_partner_id=t.data_partner_id
)
