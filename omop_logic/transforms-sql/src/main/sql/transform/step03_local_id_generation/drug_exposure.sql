CREATE TABLE `ri.foundry.main.dataset.0da5f597-e7d7-471b-8613-883c3601e1ec` AS

    with drugexp as ( 
        -- if the concept is mappable from 02_prepared_01
        SELECT
              drug_exposure_id as site_domain_id
            , 'DRUG_EXPOSURE_ID:' || drug_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 )as  drug_concept_id 
            , drug_exposure_start_date
            , drug_exposure_start_datetime
            , drug_exposure_end_date
            , drug_exposure_end_datetime
            , verbatim_end_date
            , drug_type_concept_id
            , stop_reason
            , refills
            , quantity
            , days_supply
            , sig
            , route_concept_id
            , lot_number
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , drug_source_value
            , drug_source_concept_id
            , route_source_value
            , dose_unit_source_value
            , 'DRUG' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.0e99bf0a-15ec-44db-b71d-8b711c8eab3a` d
        -- site are sending drug that may contain drug/null/device
        WHERE drug_exposure_id IS NOT NULL
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Device'), do not include them in this table
        ---e.g we are mapping the domain in Device, Procedure, Observation from Drug -  NOT IN ('Observation', 'Measurement', 'Device')
        AND d.target_concept_id IS NOT NULL
       AND (d.target_domain_id NOT IN ('Device', 'Observation', 'Measurement','Procedure','Condition','Visit') or d.target_domain_id is null)
    ),
    drug_unmap as (
        SELECT
              drug_exposure_id as site_domain_id
            , 'DRUG_EXPOSURE_ID:' || drug_exposure_id || '|DRUG_CONCEPT_ID:' || COALESCE(drug_concept_id , '') as source_pkey
            , person_id as site_person_id
            , drug_concept_id 
            , drug_exposure_start_date
            , drug_exposure_start_datetime
            , drug_exposure_end_date
            , drug_exposure_end_datetime
            , verbatim_end_date
            , drug_type_concept_id
            , stop_reason
            , refills
            , quantity
            , days_supply
            , sig
            , route_concept_id
            , lot_number
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , drug_source_value
            , drug_source_concept_id
            , route_source_value
            , dose_unit_source_value
            , 'DRUG' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.0e99bf0a-15ec-44db-b71d-8b711c8eab3a` d
        -- site are sending drug that may contain drug/null/device
        WHERE (drug_exposure_id IS NOT NULL
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Device'), do not include them in this table
        ---e.g we are mapping the domain in Device, Procedure, Observation from Drug -  NOT IN ('Observation', 'Measurement', 'Device')
        AND d.target_concept_id IS NULL)
        -- Remove the visit clause for easy understanding.
        -- If in the future, we encouter cases that are mapped to visit but are not standard, we can add an additional condition for that
       --AND (d.target_domain_id NOT IN ('Device', 'Observation', 'Measurement','Procedure','Condition') or d.target_domain_id is null)
    ),
    ---proc2drug
    procedures as (
        SELECT
            procedure_occurrence_id as site_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as drug_concept_id
            , procedure_date as drug_exposure_start_date
            , procedure_datetime as drug_exposure_start_datetime
            , procedure_date as drug_exposure_end_date
            , procedure_datetime as drug_exposure_end_datetime
            , CAST(NULL as date) verbatim_end_date
            , 32817 as drug_type_concept_id
            , CAST(NULL AS string ) as stop_reason
            , CAST(NULL AS int ) as refills
            , CAST( quantity AS float) as quantity
            , CAST(NULL AS int ) as days_supply
            , CAST(NULL AS string ) as sig
            , case 
                when procedure_source_value like '%INJECT%' THEN 4312507
                when procedure_source_value like '%FLU VACC%' OR procedure_source_value like '%POLIOVIRUS%' OR procedure_source_value like '%VACCINE%' THEN 4295880
                else CAST( null as int ) 
                END as route_concept_id
            , CAST(NULL AS string ) as lot_number
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value as drug_source_value
            , procedure_source_concept_id as drug_source_concept_id
            , procedure_source_value as route_source_value
            , procedure_source_value as dose_unit_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.8e9c4a3f-516e-4288-8ec5-946c787ddb5a` p
        WHERE procedure_occurrence_id IS NOT NULL
        --AND p.target_concept_id IS NOT NULL
        AND p.target_domain_id = 'Drug'
    ),
    condition as (
            SELECT
            condition_occurrence_id as site_domain_id
            , 'CONDITION_OCCRRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as  drug_concept_id 
            , condition_start_date as drug_exposure_start_date
            , condition_start_datetime as drug_exposure_start_datetime
            , condition_end_date as drug_exposure_end_date
            , condition_end_datetime as drug_exposure_end_datetime
            , CAST(NULL as date) as verbatim_end_date
            , 32817 as drug_type_concept_id
            , stop_reason
            , CAST(NULL AS int ) as refills
            , CAST(null AS float) as quantity
            , CAST(NULL AS int ) as days_supply
            , CAST(NULL AS string ) as sig
            , CAST( null as int ) as route_concept_id
            , CAST(NULL AS string ) as lot_number
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as drug_source_value
            , condition_source_concept_id as drug_source_concept_id
            , condition_source_value as route_source_value
            , condition_source_value as dose_unit_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.2ccc3011-db3a-47de-b6fe-4fc0908129c8` c
            where c.condition_occurrence_id is not null
            and c.target_domain_id = 'Drug'
    ),
    device as (
        SELECT
            device_exposure_id as site_domain_id
            , 'DEVICE_EXPOSURE_ID:' || device_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as  drug_concept_id 
            , device_exposure_start_date as drug_exposure_start_date
            , device_exposure_start_datetime as drug_exposure_start_datetime
            , device_exposure_end_date as drug_exposure_end_date
            , device_exposure_end_datetime as drug_exposure_end_datetime
            , CAST(NULL as date) as verbatim_end_date
            , 32817 as drug_type_concept_id
            , CAST(NULL AS string ) as stop_reason
            , CAST(NULL AS int ) as refills
            , CAST(null AS float) as quantity
            , CAST(NULL AS int ) as days_supply
            , CAST(NULL AS string ) as sig
            , CAST( null as int ) as route_concept_id
            , CAST(NULL AS string ) as lot_number
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , device_source_value as drug_source_value
            , device_source_concept_id as drug_source_concept_id
            , device_source_value as route_source_value
            , device_source_value as dose_unit_source_value
            , 'DEVICE' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.de40ba11-7679-45fe-a34c-2fb986a989a7` d
            where d.device_exposure_id is not null
            and d.target_domain_id ='Drug'
    ),
    observation as (
            SELECT
            observation_id as site_domain_id
            , 'OBSERVATION_ID:' || observation_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as  drug_concept_id 
            , observation_date as drug_exposure_start_date
            , observation_datetime as drug_exposure_start_datetime
            , observation_date as drug_exposure_end_date
            , observation_datetime as drug_exposure_end_datetime
            , CAST(NULL as date) as verbatim_end_date
            , 32817 as drug_type_concept_id
            , CAST(NULL AS string ) as stop_reason
            , CAST(NULL AS int ) as refills
            , CAST( null AS float) as quantity
            , CAST(NULL AS int ) as days_supply
            , CAST(NULL AS string ) as sig
            , CAST( null as int ) as route_concept_id
            , CAST(NULL AS string ) as lot_number
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , observation_source_value as drug_source_value
            , observation_source_concept_id as drug_source_concept_id
            , observation_source_value as route_source_value
            , observation_source_value as dose_unit_source_value
            , 'OBSERVATION' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.5f465bad-f62f-4fa7-832f-e8e8e653169b` o
            where o.observation_id is not null
            and o.target_domain_id ='Drug'
    ),
    measurement as (
            SELECT
            measurement_id as site_domain_id
            , 'MEASUREMENT_ID:' || measurement_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as  drug_concept_id 
            , measurement_date as drug_exposure_start_date
            , measurement_datetime as drug_exposure_start_datetime
            , measurement_date as drug_exposure_end_date
            , measurement_datetime as drug_exposure_end_datetime
            , CAST(NULL as date) as verbatim_end_date
            , 32817 as drug_type_concept_id
            , CAST(NULL AS string ) as stop_reason
            , CAST(NULL AS int ) as refills
            , CAST( null AS float) as quantity
            , CAST(NULL AS int ) as days_supply
            , CAST(NULL AS string ) as sig
            , CAST( null as int ) as route_concept_id
            , CAST(NULL AS string ) as lot_number
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , measurement_source_value as drug_source_value
            , measurement_source_concept_id as drug_source_concept_id
            , measurement_source_value as route_source_value
            , measurement_source_value as dose_unit_source_value
            , 'MEASUREMENT' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.d36f1ed4-2d1f-42c0-a6d4-183c77d8cf3e` m
            where m.measurement_id is not null
            and m.target_domain_id ='Drug'

    ),
    --todo check: device2drug? not needed from the template source site data
    /*
    
    rescued_concept_from_drug as (
    ----- original data where the target domain isnull find the target concept id using the relationship_ids in 
    ---- (concept poss_eq to, concept same_as to, concept replaced, RxNorm is a)
    --- and insert the rescued concepts from the deprecated concepts  
       select 
        u.site_domain_id
        , u.source_pkey || '|TARGET_CONCEPT_ID:' || COALESCE(xw.target_concept_id, '') as source_pkey
        , u.site_person_id
        , xw.target_concept_id as drug_concept_id
        , c.drug_exposure_start_date
        , c.drug_exposure_start_datetime
        , c.drug_exposure_end_date
        , c.drug_exposure_end_datetime
        , c.verbatim_end_date
        , c.drug_type_concept_id
        , c.stop_reason
        , c.refills
        , c.quantity
        , c.days_supply
        , c.sig
        , c.route_concept_id
        , c.lot_number
        , c.provider_id as site_provider_id
        , c.visit_occurrence_id as site_visit_occurrence_id
        , c.visit_detail_id
        , c.drug_source_value
        , c.drug_source_concept_id
        , c.route_source_value
        , c.dose_unit_source_value
        , 'DRUG' as source_domain
        , c.data_partner_id
        , c.payload
        from `ri.foundry.main.dataset.c647f4a5-1faa-4277-add5-8574c6f3e2e1` u
        inner join `ri.foundry.main.dataset.0e99bf0a-15ec-44db-b71d-8b711c8eab3a` c
        on c.drug_exposure_id = u.site_domain_id AND u.site_person_id = c.person_id AND c.drug_concept_id = u.unmapped_source_concept_id 
         join `ri.foundry.main.dataset.504c654b-01bd-4285-9a98-961827d926ab` xw
        on u.unmapped_source_concept_id = xw.source_concept_id
        where xw.target_domain_id = 'DRUG'
    ),
    */
  all_domain as ( 
        select 
            *,
            md5(CAST(source_pkey as string)) AS hashed_id 
        from (
            select * from drugexp 
                UNION
            select * from drug_unmap
                union   
            select * from procedures
                union 
            select * from condition
                UNION
            select * from device
                union
            select * from observation
                UNION
            select * from measurement
        )
    ) 

    SELECT 
        * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as drug_exposure_id_51_bit
    FROM (
        select * from all_domain
    )