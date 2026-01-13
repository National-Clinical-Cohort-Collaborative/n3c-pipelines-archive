CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/04 - mapping/device_exposure` AS

    with condition as 
    (
        SELECT 
            condition_occurrence_id as source_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(c.target_concept_id, 0) as device_concept_id 
            , condition_start_date as device_exposure_start_date
            , condition_start_datetime as device_exposure_start_datetime
            , condition_end_date as device_exposure_end_date
            , condition_end_datetime as device_exposure_end_datetime
            , 32817 as device_type_concept_id
            , CAST(null as string) unique_device_id     
            --, condition_status_concept_id
            --, stop_reason
            , CAST(NULL AS INT ) as quantity 
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as device_source_value
            , condition_source_concept_id as device_source_concept_id
            --, condition_status_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
            FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/condition_occurrence` c
            WHERE condition_occurrence_id IS NOT NULL
            AND c.target_domain_id = 'Device'

    ),

    procedures as (
          SELECT
            procedure_occurrence_id as source_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(p.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , p.target_concept_id as device_concept_id
            , procedure_date as device_exposure_start_date
            , procedure_datetime as device_exposure_start_datetime
            , procedure_date as device_exposure_end_date
            , procedure_datetime as device_exposure_end_datetime
            , 32817 as device_type_concept_id
            , CAST(null as string) unique_device_id     
            , CAST(NULL AS INT ) as quantity 
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value as device_source_value
            , procedure_source_concept_id as device_source_concept_id
            --, modifier_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        AND p.target_domain_id = 'Device'

    ), device as (
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/device_exposure`d
        WHERE d.device_exposure_id IS NOT NULL 
        and target_concept_id is not null
       AND (d.target_domain_id NOT IN ('Condition', 'Observation','Meas Value', 'Measurement','Procedure','Drug','Visit') or d.target_domain_id is null)
    ),
    device_unmap as(
            SELECT 
            d.device_exposure_id as site_domain_id
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/device_exposure` d
        WHERE ( d.device_exposure_id IS NOT NULL 
        and d.target_concept_id is null ) or (d.target_domain_id = 'Visit')
       --AND (d.target_domain_id NOT IN ('Condition', 'Observation', 'Measurement','Procedure','Drug') or d.target_domain_id is null)
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
            from  `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/measurement` m
            where m.measurement_id is not null
            and m.target_domain_id = 'Device'
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
            from  `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/observation` o
            where o.observation_id is not null
            and o.target_domain_id = 'Device'
    ),
    drug as (
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
    FROM  `ri.foundry.main.dataset.293d0212-2f37-4058-a2e2-f075b69c681d` d
    WHERE drug_exposure_id IS NOT NULL
    AND (d.target_domain_id = 'Device' )
    ),

    all_domain as (
        select distinct
            *, 
            md5(CAST(source_pkey as string)) AS hashed_id
        from (
            select * from condition 
                union all  
            select * from procedures
            union ALL
            select * from device
            union ALL
            select * from device_unmap
            union ALL
            select * from measurement
            union ALL
            select * from drug
            union ALL
            select * from observation
        ) 
    )

SELECT 
        * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as device_exposure_id_51_bit
FROM all_domain