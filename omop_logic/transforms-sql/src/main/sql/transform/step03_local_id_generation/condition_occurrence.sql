CREATE TABLE `ri.foundry.main.dataset.e1b241e5-9b80-485a-abab-98159245fd09` AS
    
    --condition2condition
    with condition as (
        -- if cooncept is mappable based on 02_porepared_01 extended columns
        SELECT
            condition_occurrence_id as site_domain_id
            ----SSH Note: if we are joining relationship table to grab the the full target concepts columns, 
            --- then there may be instances where the non-standard code can be mapped to multiple target_concept_id
            ----for those instances, we would need to create unique set of source_pkey. And use the source_pkey to generate the hashed_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
            -----, 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as condition_concept_id
            , condition_start_date
            , condition_start_datetime
            , condition_end_date
            , condition_end_datetime
            , condition_type_concept_id
            , condition_status_concept_id
            , stop_reason
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id as visit_detail_id
            , condition_source_value
            , condition_source_concept_id
            , condition_status_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
        -- pull the data from the prepared step where we have extended the dataset to include the target columns using the newest OMOP vocab tables
        -- Do not include data here if we are moving to other domain
        -- FROM ri.foundry.main.dataset.2ccc3011-db3a-47de-b6fe-4fc0908129c8
        FROM  `ri.foundry.main.dataset.2ccc3011-db3a-47de-b6fe-4fc0908129c8` c
        --domain id can be in Condition NULL or Observation
        WHERE c.condition_occurrence_id IS NOT NULL
        and c.target_concept_id is not null
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Observation'), do not include them in this table, condition contained target domain id in ( condition, observation, null )
        AND (target_domain_id NOT IN ('Device', 'Observation', 'Measurement','Procedure','Drug','Visit') or target_domain_id is null)
    ), 
    condition_unmap as (--condition. If the conditions are unmappable (or standard already). Use origial data
            SELECT
            condition_occurrence_id as site_domain_id
            ----SSH Note: if we are joining relationship table to grab the the full target concepts columns, 
            --- then there may be instances where the non-standard code can be mapped to multiple target_concept_id
            ----for those instances, we would need to create unique set of source_pkey. And use the source_pkey to generate the hashed_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|CONDITION_CONCEPT_ID:' || COALESCE(c.condition_concept_id, '') as source_pkey
            -----, 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id as source_pkey
            , person_id as site_person_id
            , condition_concept_id
            , condition_start_date
            , condition_start_datetime
            , condition_end_date
            , condition_end_datetime
            , condition_type_concept_id
            , condition_status_concept_id
            , stop_reason
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id as visit_detail_id
            , condition_source_value
            , condition_source_concept_id
            , condition_status_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
        -- pull the data from the prepared step where we have extended the dataset to include the target columns using the newest OMOP vocab tables
        -- Do not include data here if we are moving to other domain
        -- FROM ri.foundry.main.dataset.2ccc3011-db3a-47de-b6fe-4fc0908129c8
        FROM  `ri.foundry.main.dataset.2ccc3011-db3a-47de-b6fe-4fc0908129c8` c
        --domain id can be in Condition NULL or Observation
        WHERE (c.condition_occurrence_id IS NOT NULL
        and c.target_concept_id is null) or (c.target_domain_id = 'Visit')
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Observation'), do not include them in this table, condition contained target domain id in ( condition, observation, null )

    ), 
    --observation2condition
    observation as (
        select
            observation_id as site_domain_id
            ---shong note : then there may be instances where the non-standard code can be mapped to multiple target_concept_id
            ----for those instances, we would need to create unique set of source_pkey. And use the source_pkey to generate the hashed_id
             , 'OBSERVATION_ID:' || observation_id || '|TARGET_CONCEPT_ID:' || COALESCE(o.target_concept_id, '') as source_pkey
             , person_id as site_person_id
             , COALESCE(target_concept_id, 0 ) as condition_concept_id
             , observation_date as condition_start_date
             , observation_datetime as condition_start_datetime
             , cast(NULL as date) as condition_end_date
             , cast(Null as timestamp) as condition_end_datetime
             , observation_type_concept_id as condition_type_concept_id
             , cast( null as int) as condition_status_concept_id
             , cast( null as string ) as stop_reason
             , provider_id as site_provider_id
             , visit_occurrence_id as site_visit_occurrence_id
             , visit_detail_id as visit_detail_id
             , observation_source_value as condition_source_value
             , observation_source_concept_id as condition_source_concept_id
             , cast( null as string ) as condition_status_source_value
             , 'OBSERVATION' as source_domain
             , data_partner_id
             , payload
        FROM `ri.foundry.main.dataset.5f465bad-f62f-4fa7-832f-e8e8e653169b` o
        WHERE o.observation_id IS NOT NULL
         -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id not in obs, cond, meas,   do not include them in this table if null 
        AND target_domain_id  = 'Condition' -- deprecated concept_id will result in the null target_concept_id
    ),

    device as (
        select
            device_exposure_id as site_domain_id
            ----SSH Note: if we are joining relationship table to grab the the full target concepts columns, 
            --- then there may be instances where the non-standard code can be mapped to multiple target_concept_id
            ----for those instances, we would need to create unique set of source_pkey. And use the source_pkey to generate the hashed_id
            , 'DEVICE_EXPOSURE_ID:' || device_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(d.target_concept_id, '') as source_pkey
            -----, 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as condition_concept_id
            , device_exposure_start_date as condition_start_date
            , device_exposure_start_datetime as condition_start_datetime
            , cast(NULL as date) as condition_end_date
            , cast(Null as timestamp) as condition_end_datetime
            , device_type_concept_id as condition_type_concept_id
            , cast( null as int) as condition_status_concept_id
            , cast( null as string ) as stop_reason
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id as visit_detail_id
            , device_source_value as condition_source_value
            , device_source_concept_id as condition_source_concept_id
            , cast( null as string ) as condition_status_source_value
            , 'DEVICE' as source_domain
            , data_partner_id
            , payload
        from `ri.foundry.main.dataset.de40ba11-7679-45fe-a34c-2fb986a989a7`  d
        where d.device_exposure_id is not null 
        and target_domain_id  = 'Condition'

    ),
    measurement as (
        SELECT
            measurement_id as site_domain_id
            ----SSH Note: if we are joining relationship table to grab the the full target concepts columns, 
            --- then there may be instances where the non-standard code can be mapped to multiple target_concept_id
            ----for those instances, we would need to create unique set of source_pkey. And use the source_pkey to generate the hashed_id
            , 'MEASUREMENTE_ID:' || measurement_id || '|TARGET_CONCEPT_ID:' || COALESCE(m.target_concept_id, '') as source_pkey
            -----, 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 )as condition_concept_id
            , measurement_date as condition_start_date
            , measurement_datetime as condition_start_datetime
            , cast(NULL as date) as condition_end_date
            , cast(Null as timestamp) as condition_end_datetime
            , measurement_type_concept_id as condition_type_concept_id
            , cast( null as int) as condition_status_concept_id
            , cast( null as string ) as stop_reason
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id as visit_detail_id
            , measurement_source_value as condition_source_value
            , measurement_source_concept_id as condition_source_concept_id
            , cast( null as string ) as condition_status_source_value
            , 'MEASUREMENT' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.d36f1ed4-2d1f-42c0-a6d4-183c77d8cf3e` m
            where m.measurement_id is not NULL
            and target_domain_id  = 'Condition'
    ),
    procedure as (
        SELECT
            procedure_occurrence_id as site_domain_id
            ----SSH Note: if we are joining relationship table to grab the the full target concepts columns, 
            --- then there may be instances where the non-standard code can be mapped to multiple target_concept_id
            ----for those instances, we would need to create unique set of source_pkey. And use the source_pkey to generate the hashed_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(p.target_concept_id, '') as source_pkey
            -----, 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as condition_concept_id
            , procedure_date as condition_start_date
            , procedure_datetime as condition_start_datetime
            , cast(NULL as date) as condition_end_date
            , cast(Null as timestamp) as condition_end_datetime
            , procedure_type_concept_id as condition_type_concept_id
            , cast( null as int) as condition_status_concept_id
            , cast( null as string ) as stop_reason
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id as visit_detail_id
            , procedure_source_value as condition_source_value
            , procedure_source_concept_id as condition_source_concept_id
            , cast( null as string ) as condition_status_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        from `ri.foundry.main.dataset.8e9c4a3f-516e-4288-8ec5-946c787ddb5a` p
        where p.procedure_occurrence_id is not null
        and target_domain_id  = 'Condition'
    ),
    drug as (
        SELECT
            drug_exposure_id as site_domain_id
            ----SSH Note: if we are joining relationship table to grab the the full target concepts columns, 
            --- then there may be instances where the non-standard code can be mapped to multiple target_concept_id
            ----for those instances, we would need to create unique set of source_pkey. And use the source_pkey to generate the hashed_id
            , 'DRUG_EXPOSURE_ID:' || drug_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(dr.target_concept_id, '') as source_pkey
            -----, 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as condition_concept_id
            , drug_exposure_start_date as condition_start_date
            , drug_exposure_start_datetime as condition_start_datetime
            , drug_exposure_end_date as condition_end_date
            , drug_exposure_end_datetime as condition_end_datetime
            , drug_type_concept_id as condition_type_concept_id
            , cast( null as int) as condition_status_concept_id
            , stop_reason
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id as visit_detail_id
            , drug_source_value as condition_source_value
            , drug_source_concept_id as condition_source_concept_id
            , cast( null as string ) as condition_status_source_value
            , 'DRUG' as source_domain
            , data_partner_id
            , payload
        from  `ri.foundry.main.dataset.0e99bf0a-15ec-44db-b71d-8b711c8eab3a` dr
        where dr.drug_exposure_id is not null
        and target_domain_id  = 'Condition'

    ),
    all_domain as (
        select 
        *, 
        md5(CAST(source_pkey as string )) as hashed_id
        from (
            select * from condition
            union 
            select * from condition_unmap
            union
            select * from observation
            union 
            select * from device
            union
            select * from measurement
            union
            select * from procedure
            UNION
            select * from drug
        --select * from unmapped
         -- select * from rescued_concept_from_condition
        )
    )

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as condition_occurrence_id_51_bit
    FROM  all_domain 