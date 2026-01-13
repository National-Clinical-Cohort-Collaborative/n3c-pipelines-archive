CREATE TABLE `ri.foundry.main.dataset.6a46a32b-27da-486d-9162-a5ace12fe004` AS

    --obs2obs
    with observation as
    (
           SELECT
            observation_id as site_domain_id
            , 'OBSERVATION_ID:' || observation_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE( target_concept_id, 0) as observation_concept_id -- if the target_concept_id is null use what was submitted in this column
            , observation_date
            , observation_datetime
            , observation_type_concept_id
            , value_as_number
            , value_as_string
            , value_as_concept_id
            , qualifier_concept_id
            , unit_concept_id
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , observation_source_value
            , observation_source_concept_id
            , unit_source_value
            , qualifier_source_value
            ,'OBSERVATION' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.5f465bad-f62f-4fa7-832f-e8e8e653169b` o
        WHERE observation_id IS NOT NULL 
        and target_concept_id is not null
        --AND o.target_concept_id is not null -- deprecated concept_id will result in the null target_concept_id
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Measurement' OR 'Condition'), do not include them in this table
       -- AND (o.target_domain_id IS NULL or o.target_domain_id NOT IN ('Measurement', 'Condition')) --- need to capture all other domain, unless we are mapping them to another domain
        --AND o.target_domain_id NOT IN ('Measurement', 'Condition') --- all other target domain stays in Observation unless we are mapping them. 
        AND (o.target_domain_id NOT IN ('Device', 'Condition', 'Measurement','Procedure','Drug','Visit') or o.target_domain_id is null)
    ), 
    observation_unmap as(
        SELECT
        observation_id as site_domain_id
        , 'OBSERVATION_ID:' || observation_id || '|OBSERVATION_CONCEPT_ID:' || COALESCE(observation_concept_id, '') as source_pkey
        , person_id as site_person_id
        , observation_concept_id -- if the target_concept_id is null use what was submitted in this column
        , observation_date
        , observation_datetime
        , observation_type_concept_id
        , value_as_number
        , value_as_string
        , value_as_concept_id
        , qualifier_concept_id
        , unit_concept_id
        , provider_id as site_provider_id
        , visit_occurrence_id as site_visit_occurrence_id
        , visit_detail_id
        , observation_source_value
        , observation_source_concept_id
        , unit_source_value
        , qualifier_source_value
        ,'OBSERVATION' as source_domain
        , data_partner_id
        , payload
    FROM `ri.foundry.main.dataset.5f465bad-f62f-4fa7-832f-e8e8e653169b` o
    WHERE (observation_id IS NOT NULL 
    and target_concept_id is null) 
    -- Remove the visit clause for easy understanding.
    -- If in the future, we encouter cases that are mapped to visit but are not standard, we can add an additional condition for that
    ),

    --condition2obs
    condition as (
        select 
            condition_occurrence_id as site_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE( target_concept_id, 0) as observation_concept_id
            , condition_start_date as observation_date
            , condition_start_datetime as observation_datetime
            , condition_type_concept_id as observation_type_concept_id
            , CAST(null as int ) as value_as_number
            , CAST(null as string ) as value_as_string
            , CAST(null as int ) as value_as_concept_id
            , CAST(null as int ) as qualifier_concept_id
            , CAST(null as int ) as unit_concept_id
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as observation_source_value
            , condition_source_concept_id as observation_source_concept_id
            , CAST(null as string ) as unit_source_value
            , CAST(null as string ) as qualifier_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.2ccc3011-db3a-47de-b6fe-4fc0908129c8` c
        WHERE condition_occurrence_id IS NOT NULL
        --AND c.target_concept_id IS NOT NULL -- deprecated concept_id will result in the null target_concept_id
        And c.target_domain_id = 'Observation'

    ), 

    --procedures2observation
    procedures as (
        SELECT 
            procedure_occurrence_id as site_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE( target_concept_id, 0) as observation_concept_id
            , procedure_date as observation_date
            , procedure_datetime as observation_datetime
            , procedure_type_concept_id as observation_type_concept_id
            , CAST(null as float) AS value_as_number
            , CAST(null as string) AS value_as_string
            , CAST(null as int) AS value_as_concept_id
            , CAST(null as int) AS qualifier_concept_id
            , CAST(null as int) AS unit_concept_id
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value as observation_source_value
            , procedure_source_concept_id as observation_source_concept_id
            , CAST( NULL AS string) as unit_source_value
            , CAST( NULL AS string) as qualifier_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.8e9c4a3f-516e-4288-8ec5-946c787ddb5a` p
        WHERE procedure_occurrence_id IS NOT NULL
        --AND p.target_concept_id IS NOT NULL -- deprecated concept_id will result in the null target_concept_id
        And p.target_domain_id = 'Observation'
    ),
    --from drug -- drug2obs
    drugexp as (
        SELECT 
            drug_exposure_id as site_domain_id
            , 'DRUG_EXPOSURE_ID:' || drug_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE( target_concept_id, 0) as observation_concept_id
            , drug_exposure_start_date as observation_date
            , drug_exposure_start_datetime as observation_datetime
            , drug_type_concept_id as observation_type_concept_id
            , CAST(null as float) AS value_as_number
            , CAST(null as string) AS value_as_string
            , CAST(null as int) AS value_as_concept_id
            , CAST(null as int) AS qualifier_concept_id
            , CAST(null as int) AS unit_concept_id
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , drug_source_value as observation_source_value
            , drug_source_concept_id as observation_source_concept_id
            , CAST( dose_unit_source_value AS string) as unit_source_value
            , CAST( NULL AS string) as qualifier_source_value
            , 'Drug' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.0e99bf0a-15ec-44db-b71d-8b711c8eab3a` dr
        WHERE drug_exposure_id IS NOT NULL
        --AND d.target_concept_id IS NOT NULL -- deprecated concept_id will result in the null target_concept_id
        And dr.target_domain_id = 'Observation'

    ),
    device as (
        SELECT
        device_exposure_id as site_domain_id
        , 'DEVICE_EXPOSURE_ID:' || device_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
        , person_id as site_person_id
        , COALESCE( target_concept_id, 0) as observation_concept_id -- if the target_concept_id is null use what was submitted in this column
        , device_exposure_start_date as observation_date
        , device_exposure_start_datetime as observation_datetime
        , device_type_concept_id as observation_type_concept_id
        , CAST(null as float) AS value_as_number
        , CAST(null as string) AS value_as_string
        , CAST(null as int) ASvalue_as_concept_id
        , CAST(null as int) AS qualifier_concept_id
        , CAST(null as int) AS unit_concept_id
        , provider_id as site_provider_id
        , visit_occurrence_id as site_visit_occurrence_id
        , visit_detail_id
        , device_source_value as observation_source_value
        , device_source_concept_id as observation_source_concept_id
        , CAST( NULL AS string) as unit_source_value
        , CAST( NULL AS string) as qualifier_source_value
        ,'DEVICE' as source_domain
        , data_partner_id
        , payload
        from `ri.foundry.main.dataset.de40ba11-7679-45fe-a34c-2fb986a989a7` d
        where d.device_exposure_id is not null
        and d.target_domain_id = 'Observation'
    ),
    measurement as (
        SELECT
            measurement_id as site_domain_id
            , 'MEASUREMENT_ID:' || measurement_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE( target_concept_id, 0) as observation_concept_id -- if the target_concept_id is null use what was submitted in this column
            , measurement_date as observation_date
            , measurement_datetime as observation_datetime
            , measurement_type_concept_id as observation_type_concept_id
            , value_as_number as value_as_number
            , CAST(null as string) AS value_as_string
            , value_as_concept_id
            , CAST(null as int) AS qualifier_concept_id
            , unit_concept_id
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , measurement_source_value as observation_source_value
            , measurement_source_concept_id as observation_source_concept_id
            , unit_source_value
            , CAST( NULL AS string) as qualifier_source_value
            ,'MEASUREMENT' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.d36f1ed4-2d1f-42c0-a6d4-183c77d8cf3e` m
            where m.measurement_id is not NULL
            and m.target_domain_id = 'Observation'

    ),


    /*
    ---rescued_from_the deprecated concepts
    rescued_concept_from_observation as (
    ----- original data where the target domain is null find the target concept id using the relationship_ids in 
    ---- (concept poss_eq to, concept same_as to, concept replaced, RxNorm is a)
    --- and inserescued_concept_from_observationrt the rescued concepts from the deprecated concepts  
       select 
        u.site_domain_id
        , u.source_pkey || '|TARGET_CONCEPT_ID:' || COALESCE(xw.target_concept_id, '') as source_pkey
        , u.site_person_id
        , xw.target_concept_id as observation_concept_id
        , o.observation_date
        , o.observation_datetime
        , o.observation_type_concept_id
        , o.value_as_number
        , o.value_as_string
        , o.value_as_concept_id
        , o.qualifier_concept_id
        , o.unit_concept_id
        , o.provider_id as site_provider_id
        , o.visit_occurrence_id as site_visit_occurrence_id
        , o.visit_detail_id
        , o.observation_source_value
        , xw.source_concept_id as observation_source_concept_id
        , o.unit_source_value
        , o.qualifier_source_value
        ,'OBSERVATION' as source_domain
        , o.data_partner_id
        , o.payload
        from `ri.foundry.main.dataset.c647f4a5-1faa-4277-add5-8574c6f3e2e1` u
        inner join `ri.foundry.main.dataset.5f465bad-f62f-4fa7-832f-e8e8e653169b`  o
        on o.observation_id = u.site_domain_id AND u.site_person_id = o.person_id AND o.observation_concept_id = u.unmapped_source_concept_id 
         join `ri.foundry.main.dataset.504c654b-01bd-4285-9a98-961827d926ab` xw
        on u.unmapped_source_concept_id = xw.source_concept_id
        where xw.target_domain_id = 'Observation'
    ),
    unmapped as (
        select 
        u.site_domain_id
        , u.source_pkey || '|TARGET_CONCEPT_ID:' || COALESCE(o.target_concept_id, '') as source_pkey
        , u.site_person_id
        ,  COALESCE(o.target_concept_id, o.observation_concept_id ) as observation_concept_id
        , o.observation_date
        , o.observation_datetime
        , o.observation_type_concept_id
        , o.value_as_number
        , o.value_as_string
        , o.value_as_concept_id
        , o.qualifier_concept_id
        , o.unit_concept_id
        , o.provider_id as site_provider_id
        , o.visit_occurrence_id as site_visit_occurrence_id
        , o.visit_detail_id
        , o.observation_source_value
        , o.observation_source_concept_id
        , o.unit_source_value
        , o.qualifier_source_value
        ,'OBSERVATION' as source_domain
        , o.data_partner_id
        , o.payload
        from `ri.foundry.main.dataset.c647f4a5-1faa-4277-add5-8574c6f3e2e1` u
        inner join `ri.foundry.main.dataset.5f465bad-f62f-4fa7-832f-e8e8e653169b`  o
        on o.observation_id = u.site_domain_id AND u.site_person_id = o.person_id AND o.observation_concept_id = u.unmapped_source_concept_id 
        where u.unmapped_source_concept_id not in(
            select source_concept_id from `ri.foundry.main.dataset.504c654b-01bd-4285-9a98-961827d926ab` xw
            where target_domain_id = 'Observation'

        ) ),

        */
    all_domain as (
        select distinct * from observation
        union
        select * from observation_unmap
        union
        select distinct * from condition
        union 
        select distinct * from procedures
        UNION
        select distinct * from drugexp
        union 
        select distinct * from device
        union
        select distinct * from measurement
        --select * from unmapped
        --select * from rescued_concept_from_observation

    ),

    final_table as (
        select
            *
            , md5( concat_ws( ';'
             , CAST(source_pkey as string)
             , COALESCE( CAST( observation_concept_id as string), ''  )
             , COALESCE( CAST( observation_source_concept_id as string), ''  )
            )) as hashed_id
        from all_domain 
    )

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_id_51_bit
    FROM (
        SELECT *
        FROM final_table
    )   