CREATE TABLE `ri.foundry.main.dataset.8e4ca1b5-475d-4272-833a-1ae1834f45e2` AS

    --measurement2measurement
    with measurement as (
         SELECT
              measurement_id as site_domain_id
            , 'MEASUREMENT_ID:' || measurement_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE( target_concept_id, 0) as measurement_concept_id
            , measurement_date
            , measurement_datetime
            , measurement_time
            , measurement_type_concept_id
            , operator_concept_id
            , value_as_number
            , value_as_concept_id
            , unit_concept_id
            , range_low
            , range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , measurement_source_value
            , measurement_source_concept_id
            , unit_source_value
            , value_source_value
            , 'MEASUREMENT' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.d36f1ed4-2d1f-42c0-a6d4-183c77d8cf3e` m
        WHERE measurement_id IS NOT NULL
        and target_concept_id is not null
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Procedure'), do not include them in this table
        -- target_domain_id of measurement are Measurement,  null, procedure, provider, condition
        AND (m.target_domain_id NOT IN ('Device', 'Observation', 'Condition','Procedure','Drug','Visit') or m.target_domain_id is null)
        
    ), 
    measurement_unmap as (
        SELECT
            measurement_id as site_domain_id
        , 'MEASUREMENT_ID:' || measurement_id || '|MEASUREMENT_CONCEPT_ID:' || COALESCE(measurement_concept_id, '') as source_pkey
        , person_id as site_person_id
        , measurement_concept_id
        , measurement_date
        , measurement_datetime
        , measurement_time
        , measurement_type_concept_id
        , operator_concept_id
        , value_as_number
        , value_as_concept_id
        , unit_concept_id
        , range_low
        , range_high
        , provider_id as site_provider_id
        , visit_occurrence_id as site_visit_occurrence_id
        , visit_detail_id
        , measurement_source_value
        , measurement_source_concept_id
        , unit_source_value
        , value_source_value
        , 'MEASUREMENT' as source_domain
        , data_partner_id
        , payload
    FROM `ri.foundry.main.dataset.d36f1ed4-2d1f-42c0-a6d4-183c77d8cf3e` m
    WHERE (measurement_id IS NOT NULL
    and target_concept_id is null) or (m.target_domain_id = 'Visit')
    ),

    --procedures2measurement
    procedures as (
        SELECT
            procedure_occurrence_id as site_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE( target_concept_id, 0) as measurement_concept_id
            , procedure_date as measurement_date
            , CAST(procedure_datetime as timestamp) as measurement_datetime 
            , CAST( NULL AS string ) as measurement_time
            , 32817 as measurement_type_concept_id
            , modifier_concept_id as operator_concept_id
            , CAST( NULL as float ) as value_as_number
            , CAST( NULL as int ) as value_as_concept_id
            , CAST( NULL as int ) as unit_concept_id
            , CAST( NULL as float ) as range_low
            , CAST( NULL as float ) as range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value as measurement_source_value
            , procedure_source_concept_id as measurement_source_concept_id
            , modifier_source_value as unit_source_value
            , procedure_source_value as value_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.8e9c4a3f-516e-4288-8ec5-946c787ddb5a` p
        WHERE procedure_occurrence_id IS NOT NULL
        AND p.target_domain_id = 'Measurement'
    ),
    
    --observation2measurement
    observation as (
        SELECT
            observation_id as site_domain_id
            , 'OBSERVATION_ID:' || observation_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE( target_concept_id, 0) as measurement_concept_id
            , o.observation_date as measurement_date
            , CAST(o.observation_datetime as timestamp) as measurement_datetime
            , CAST(null as string) AS measurement_time
            , observation_type_concept_id as measurement_type_concept_id
            , cast(null as int) as operator_concept_id
            , value_as_number
            , value_as_concept_id
            , unit_concept_id
            , CAST( NULL as float ) as range_low
            , CAST( NULL as float ) as range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , observation_source_value as measurement_source_value
            , observation_source_concept_id as measurement_source_concept_id
            , unit_source_value
            , CAST(null as string) as value_source_value
            , 'OBSERVATION' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.5f465bad-f62f-4fa7-832f-e8e8e653169b` o
        WHERE observation_id IS NOT NULL
        AND o.target_domain_id = 'Measurement'
    ),

    condition as (
        SELECT
              condition_occurrence_id as site_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE( target_concept_id, 0) as measurement_concept_id
            , condition_start_date as measurement_date
            , condition_start_datetime as measurement_datetime
            , CAST(null as string) AS measurement_time
            , condition_type_concept_id as measurement_type_concept_id
            , cast(null as int) as operator_concept_id
            , CAST( NULL as float ) as value_as_number
            , CAST( NULL as int ) as value_as_concept_id
            , CAST( NULL as int ) as unit_concept_id
            , CAST( NULL as float ) as range_low
            , CAST( NULL as float ) as range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as measurement_source_value
            , condition_source_concept_id as measurement_source_concept_id
            , cast( NULL as string) as unit_source_value
            , cast( NULL as string) as value_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.2ccc3011-db3a-47de-b6fe-4fc0908129c8` c
            where c.condition_occurrence_id is not null
            and c.target_domain_id = 'Measurement'
        
    ),
    device as (
        SELECT
            device_exposure_id as site_domain_id
            , 'DEVICE_EXPOSURE_ID:' || device_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE( target_concept_id, 0) as measurement_concept_id
            , device_exposure_start_date as measurement_date
            , device_exposure_start_datetime as measurement_datetime
            , CAST( NULL AS string ) as measurement_time
            , device_type_concept_id as measurement_type_concept_id
            , cast(null as int) as operator_concept_id
            , CAST( NULL as float ) as value_as_number
            , CAST( NULL as int ) as value_as_concept_id
            , CAST( NULL as int ) as unit_concept_id
            , CAST( NULL as float ) as range_low
            , CAST( NULL as float ) as range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , device_source_value as measurement_source_value
            , device_source_concept_id as measurement_source_concept_id
            , cast( NULL as string) as unit_source_value
            , cast( NULL as string) as value_source_value
            , 'DEVICE' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.de40ba11-7679-45fe-a34c-2fb986a989a7` d
            where d.device_exposure_id is not null
            and d.target_domain_id = 'Measurement'
    ),
    drug as (
        SELECT
        drug_exposure_id as site_domain_id
        , 'DRUG_EXPOSURE_ID:' || drug_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
        , person_id as site_person_id
        , COALESCE( target_concept_id, 0) as measurement_concept_id
        , drug_exposure_start_date as measurement_date
        , drug_exposure_start_datetime as measurement_datetime
        , CAST( NULL AS string ) asmeasurement_time
        , drug_type_concept_id as measurement_type_concept_id
        , cast(null as int) as operator_concept_id
        , CAST( NULL as float ) as value_as_number
        , CAST( NULL as int ) as value_as_concept_id
        , CAST( NULL as int ) as unit_concept_id
        , CAST( NULL as float ) as range_low
        , CAST( NULL as float ) as range_high
        , provider_id as site_provider_id
        , visit_occurrence_id as site_visit_occurrence_id
        , visit_detail_id
        , drug_source_value as measurement_source_value
        , drug_source_concept_id as measurement_source_concept_id
        , dose_unit_source_value as unit_source_value
        , cast( NULL as string) as value_source_value
        , 'DRUG' as source_domain
        , data_partner_id
        , payload
        from `ri.foundry.main.dataset.0e99bf0a-15ec-44db-b71d-8b711c8eab3a` dr
        where dr.drug_exposure_id is not null
        and dr.target_domain_id = 'Measurement'

    ),


/*

    ---rescued_from_the deprecated concepts
    rescued_concept_from_measurement as (
    ----- original data where the target domain is null find the target concept id using the relationship_ids in 
    ---- (concept poss_eq to, concept same_as to, concept replaced, RxNorm is a)
    --- and insert the rescued concepts from the deprecated concepts  
       select 
        u.site_domain_id
        , u.source_pkey || '|TARGET_CONCEPT_ID:' || COALESCE(xw.target_concept_id, '') as source_pkey
        , u.site_person_id
        ,xw.target_concept_id as measurement_concept_id
        , m.measurement_date
        , m.measurement_datetime
        , m.measurement_time
        , m.measurement_type_concept_id
        , m.operator_concept_id
        , m.value_as_number
        , m.value_as_concept_id
        , m.unit_concept_id
        , m.range_low
        , m.range_high
        , m.provider_id as site_provider_id
        , m.visit_occurrence_id as site_visit_occurrence_id
        , m.visit_detail_id as visit_detail_id
        , m.measurement_source_value
        , xw.source_concept_id as measurement_source_concept_id
        , m.unit_source_value
        , m.value_source_value
        , 'MEASUREMENT' as source_domain
        , m.data_partner_id
        , m.payload
        from `ri.foundry.main.dataset.c647f4a5-1faa-4277-add5-8574c6f3e2e1` u
        inner join `ri.foundry.main.dataset.d36f1ed4-2d1f-42c0-a6d4-183c77d8cf3e` m
        on m.measurement_id = u.site_domain_id AND u.site_person_id = m.person_id AND m.measurement_concept_id = u.unmapped_source_concept_id 
         join `ri.foundry.main.dataset.504c654b-01bd-4285-9a98-961827d926ab` xw
        on u.unmapped_source_concept_id = xw.source_concept_id
        where xw.target_domain_id = 'Measurement'
    ),
    unmapped as (
        select 
        u.site_domain_id
        , u.source_pkey || '|TARGET_CONCEPT_ID:' || COALESCE(m.target_concept_id, '') as source_pkey
        , u.site_person_id
        ,  COALESCE(m.target_concept_id, m.measurement_concept_id ) as measurement_concept_id
        , m.measurement_date
        , m.measurement_datetime
        , m.measurement_time
        , m.measurement_type_concept_id
        , m.operator_concept_id
        , m.value_as_number
        , m.value_as_concept_id
        , m.unit_concept_id
        , m.range_low
        , m.range_high
        , m.provider_id as site_provider_id
        , m.visit_occurrence_id as site_visit_occurrence_id
        , m.visit_detail_id
        , m.measurement_source_value
        , m.measurement_source_concept_id
        , m.unit_source_value
        , m.value_source_value
        , 'MEASUREMENT' as source_domain
        , m.data_partner_id
        , m.payload
        from `ri.foundry.main.dataset.c647f4a5-1faa-4277-add5-8574c6f3e2e1` u
        inner join `ri.foundry.main.dataset.d36f1ed4-2d1f-42c0-a6d4-183c77d8cf3e` m
        on m.measurement_id = u.site_domain_id AND u.site_person_id = m.person_id AND m.measurement_concept_id = u.unmapped_source_concept_id 
        where u.unmapped_source_concept_id not in(
            select source_concept_id from `ri.foundry.main.dataset.504c654b-01bd-4285-9a98-961827d926ab` xw
            where target_domain_id = 'Measurement'

        ) ),

*/        
    all_domain as ( 
        select
            *,
            md5(CAST(source_pkey as string)) AS hashed_id 
        from (
            select * from measurement 
            UNION
            select * from measurement_unmap
            union 
            select * from procedures
            union
            select * from observation
            union 
            select * from condition
            union
            select * from device
            UNION
            select * from drug
            --select * from unmapped
            --select * from rescued_concept_from_measurement
        )
    ) 

    SELECT 
            * 
            , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as measurement_id_51_bit
    FROM (
        select * from all_domain
         )   