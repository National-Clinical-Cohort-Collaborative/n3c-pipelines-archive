CREATE TABLE `ri.foundry.main.dataset.813f1ad9-71b0-43ba-8692-5de0bb3b66d3` AS

    ---drug2device
    with drug as (

         SELECT
              drug_exposure_id as site_domain_id
            , 'DRUG_EXPOSURE_ID:' || drug_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '')  as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as device_concept_id
            , drug_exposure_start_date as device_exposure_start_date
            , drug_exposure_start_datetime as device_exposure_start_datetime
            , drug_exposure_end_date as device_exposure_end_date
            , drug_exposure_end_datetime as device_exposure_end_datetime
            , coalesce(drug_type_concept_id, 32817) as device_type_concept_id
            , CAST(null as string) unique_device_id     
            , CAST(NULL AS INT ) as quantity 
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , drug_source_value as device_source_value
            , drug_source_concept_id as device_source_concept_id
            , 'DRUG' as source_domain
            , data_partner_id
            , payload
    FROM `ri.foundry.main.dataset.0e99bf0a-15ec-44db-b71d-8b711c8eab3a` d
    WHERE drug_exposure_id IS NOT NULL
    AND (d.target_domain_id = 'Device' )
    ),
    -- if concept is not mappable (or standard already)
    device_unmap as (
    -- if cooncept is mappable based on 02_porepared_01 extended columns
       SELECT d.device_exposure_id as site_domain_id
            , 'DEVICE_EXPOSURE_ID:' || device_exposure_id || '|DEVICE_CONCEPT_ID:' || COALESCE(d.device_concept_id, '') as source_pkey
            , d.person_id as site_person_id
            , device_concept_id
            , d.device_exposure_start_date
            , d.device_exposure_start_datetime
            , d.device_exposure_end_date
            , d.device_exposure_end_datetime
            , coalesce(d.device_type_concept_id, 32817 ) as device_type_concept_id
            , d.unique_device_id     
            , d.quantity 
            , d.provider_id as site_provider_id
            , d.visit_occurrence_id as site_visit_occurrence_id
            , d.visit_detail_id as site_visit_detail_id
            , d.device_source_value
            , d.device_source_concept_id
            , 'DEVICE' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.de40ba11-7679-45fe-a34c-2fb986a989a7` d
        WHERE ( d.device_exposure_id IS NOT NULL 
        and d.target_concept_id is null ) or (d.target_domain_id = 'Visit')
       --AND (d.target_domain_id NOT IN ('Condition', 'Observation', 'Measurement','Procedure','Drug') or d.target_domain_id is null)
    ),

    --procedure2device
    procedure as (
        SELECT
            procedure_occurrence_id as site_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as device_concept_id
            , procedure_date as device_exposure_start_date
            , procedure_datetime as device_exposure_start_datetime
            , procedure_date as device_exposure_end_date
            , procedure_datetime as device_exposure_end_datetime
            , coalesce(procedure_type_concept_id, 32817 ) as device_type_concept_id
            , CAST(null as string) as unique_device_id     
            , CAST(NULL AS INT ) as quantity 
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value as device_source_value
            , procedure_source_concept_id as device_source_concept_id
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.8e9c4a3f-516e-4288-8ec5-946c787ddb5a` p
        WHERE procedure_occurrence_id IS NOT NULL 
        AND p.target_domain_id = 'Device'
    ),

    --device2device 
    -- if cooncept is mappable based on 02_porepared_01 extended columns
    device as (
        SELECT d.device_exposure_id as site_domain_id
            , 'DEVICE_EXPOSURE_ID:' || device_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(d.target_concept_id, '') as source_pkey
            , d.person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as device_concept_id
            , d.device_exposure_start_date
            , d.device_exposure_start_datetime
            , d.device_exposure_end_date
            , d.device_exposure_end_datetime
            , coalesce(d.device_type_concept_id, 32817 ) as device_type_concept_id
            , d.unique_device_id     
            , d.quantity 
            , d.provider_id as site_provider_id
            , d.visit_occurrence_id as site_visit_occurrence_id
            , d.visit_detail_id as site_visit_detail_id
            , d.device_source_value
            , d.device_source_concept_id
            , 'DEVICE' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.de40ba11-7679-45fe-a34c-2fb986a989a7` d
        WHERE d.device_exposure_id IS NOT NULL 
        and target_concept_id is not null
       AND (d.target_domain_id NOT IN ('Condition', 'Observation', 'Measurement','Procedure','Drug','Visit') or d.target_domain_id is null)
    ),
    condition as (
        SELECT 
            condition_occurrence_id as site_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as device_concept_id
            , condition_start_date as device_exposure_start_date
            , condition_start_datetime as device_exposure_start_datetime
            , condition_end_date as device_exposure_end_date
            , condition_end_datetime as device_exposure_end_datetime
            , coalesce(condition_type_concept_id, 32817)  as device_type_concept_id
            , CAST(null as string) as unique_device_id     
            , CAST(NULL AS INT ) as quantity 
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id as site_visit_detail_id
            , condition_source_value as device_source_value
            , condition_source_concept_id as device_source_concept_id
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.2ccc3011-db3a-47de-b6fe-4fc0908129c8` c
            where condition_occurrence_id is not NULL
            and c.target_domain_id = 'Device'

    ),
    observation as (
        SELECT 
            observation_id as site_domain_id
            , 'OBSERVATION_ID:' || observation_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as device_concept_id
            , observation_date as device_exposure_start_date
            , observation_datetime as device_exposure_start_datetime
            , cast(null as date) as device_exposure_end_date
            , cast(Null as timestamp) as device_exposure_end_datetime
            , coalesce(observation_type_concept_id, 32817 ) as device_type_concept_id
            , CAST(null as string) as unique_device_id     
            , CAST(NULL AS INT ) as quantity 
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id as site_visit_detail_id
            , observation_source_value as device_source_value
            , observation_source_concept_id as device_source_concept_id
            , 'OBSERVATION' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.5f465bad-f62f-4fa7-832f-e8e8e653169b` o
            where o.observation_id is not null
            and o.target_domain_id = 'Device'
    ),
    measurement as (
        SELECT 
            measurement_id as site_domain_id
            , 'MEASUREMENT_ID:' || measurement_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as device_concept_id
            , measurement_date as device_exposure_start_date
            , measurement_datetime as device_exposure_start_datetime
            , cast(null as date) as device_exposure_end_date
            , cast(Null as timestamp) as device_exposure_end_datetime
            , coalesce(measurement_type_concept_id, 32817 ) as device_type_concept_id
            , CAST(null as string) as unique_device_id     
            , CAST(NULL AS INT ) as quantity 
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id as site_visit_detail_id
            , measurement_source_value as device_source_value
            , measurement_source_concept_id as device_source_concept_id
            , 'MEASUREMENT' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.d36f1ed4-2d1f-42c0-a6d4-183c77d8cf3e` m
            where m.measurement_id is not null
            and m.target_domain_id = 'Device'

    ),

    all_domain as (
        select *
         , md5(CAST(source_pkey as string)) as hashed_id
         from (
             select * from drug
             union
             select * from procedure
             union
             select * from device
             UNION
             select * from device_unmap
             UNION
             select * from condition
             union
             select * from observation
             union
             select * from measurement
         )
    )

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as device_exposure_id_51_bit
    FROM (
        SELECT * from all_domain
    )   