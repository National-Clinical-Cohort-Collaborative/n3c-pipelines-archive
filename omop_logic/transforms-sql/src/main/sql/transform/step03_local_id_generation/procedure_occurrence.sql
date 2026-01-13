CREATE TABLE `ri.foundry.main.dataset.184e67c2-db8d-4476-addd-58eca4ab5d89` AS
    --procedure2procedure
    with procedure as 
    (
           SELECT
              procedure_occurrence_id as site_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0) as procedure_concept_id
            , procedure_date
            , procedure_datetime
            , procedure_type_concept_id
            , modifier_concept_id
            , quantity
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , procedure_source_value
            , procedure_source_concept_id
            , modifier_source_value
            , 'PROCEDURE' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.8e9c4a3f-516e-4288-8ec5-946c787ddb5a` p
        WHERE procedure_occurrence_id IS NOT NULL
        and target_concept_id is not null
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Observation' OR 'Measurement'...), do not include them in this table
        -- e.g. Need to include visit, provider or null domain id coming through this domain also the target domain included ( procedure, visit, provider, drug, observation, measurement and null)
        --AND p.target_domain_id NOT IN ('Observation', 'Measurement', 'Drug', 'Device')
        AND (p.target_domain_id NOT IN ('Device', 'Observation', 'Measurement','Condition','Drug','Visit') or p.target_domain_id is null)
     
    ), 
    procedure_unmap as (
    SELECT
     procedure_occurrence_id as site_domain_id
    , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|PROCEDURE_CONCEPT_ID:' || COALESCE(procedure_concept_id, '') as source_pkey
    , person_id as site_person_id
    , procedure_concept_id
    , procedure_date
    , procedure_datetime
    , procedure_type_concept_id
    , modifier_concept_id
    , quantity
    , provider_id as site_provider_id
    , visit_occurrence_id as site_visit_occurrence_id
    , visit_detail_id
    , procedure_source_value
    , procedure_source_concept_id
    , modifier_source_value
    , 'PROCEDURE' as source_domain
    , data_partner_id
    , payload
    FROM `ri.foundry.main.dataset.8e9c4a3f-516e-4288-8ec5-946c787ddb5a` p
    WHERE (procedure_occurrence_id IS NOT NULL
    -- concepts that are unmappable OR concepts that are mapped to visit domain but not a standard concept
    -- concepts that are mapped to visit domain will be moved to the visit_occurrence table
    and target_concept_id is null)
    -- Remove the visit clause for easy understanding.
    -- If in the future, we encouter cases that are mapped to visit but are not standard, we can add an additional condition for that

    ),
    --measurement2procedures
    measurement as (
        select 
            measurement_id as site_domain_id
            , 'MEASUREMENT_ID:' || measurement_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0) as procedure_concept_id
            , measurement_date as procedure_date
            , measurement_datetime as procedure_datetime
            , measurement_type_concept_id as procedure_type_concept_id
            , value_as_concept_id as modifier_concept_id
            , cast( null as int ) as quantity
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , measurement_source_value as procedure_source_value
            , measurement_source_concept_id as procedure_source_concept_id
            , unit_source_value as modifier_source_value
            , 'MEASUREMENT' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.d36f1ed4-2d1f-42c0-a6d4-183c77d8cf3e` m
        WHERE measurement_id IS NOT NULL 
        --AND m.target_concept_id IS NOT NULL
        AND m.target_domain_id = 'Procedure' 
    ), 
    -- from drug2procedure
    drug as (
        select 
             drug_exposure_id as site_domain_id
            , 'DRUG_EXPOSURE_ID:' || drug_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(d.target_concept_id, 0) as  procedure_concept_id 
            , drug_exposure_start_date as procedure_date
            , drug_exposure_start_datetime as procedure_datetime
            , drug_type_concept_id as procedure_type_concept_id
            , cast( null as int ) as modifier_concept_id
            , cast( quantity as int ) as quantity
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , drug_source_value as procedure_source_value
            , drug_source_concept_id as procedure_source_concept_id
            , dose_unit_source_value as modifier_source_value
            , 'DRUG' as source_domain
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.0e99bf0a-15ec-44db-b71d-8b711c8eab3a` d
        -- site are sending drug that may contain drug/null/device
        WHERE drug_exposure_id IS NOT NULL 
        --AND d.target_concept_id IS NOT NULL
        AND d.target_domain_id = 'Procedure'
    ),
    condition as (
        SELECT
            condition_occurrence_id as site_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(c.target_concept_id, 0) as procedure_concept_id
            , condition_start_date as procedure_date
            , condition_start_datetime as procedure_datetime
            , condition_type_concept_id as procedure_type_concept_id
            , cast( null as int ) as modifier_concept_id
            , cast( null as int ) as quantity
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as procedure_source_value
            , condition_source_concept_id as procedure_source_concept_id
            , cast (null as string) as modifier_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.2ccc3011-db3a-47de-b6fe-4fc0908129c8` c
            where c.condition_occurrence_id is not NULL
            and c.target_domain_id = 'Procedure'

    ),
    device as (
        SELECT
            device_exposure_id as site_domain_id
            , 'DEVICE_EXPOSURE_ID:' || device_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0) as procedure_concept_id
            , device_exposure_start_date as procedure_date
            , device_exposure_start_datetime as procedure_datetime
            , device_type_concept_id as procedure_type_concept_id
            , cast( null as int ) as modifier_concept_id
            , quantity
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , device_source_value as procedure_source_value
            , device_source_concept_id as procedure_source_concept_id
            , cast (null as string) as modifier_source_value
            , 'DEVICE' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.de40ba11-7679-45fe-a34c-2fb986a989a7` d
            where d.device_exposure_id is not null
            and d.target_domain_id = 'Procedure'

    ),
    observation as (
         SELECT
            observation_id as site_domain_id
            , 'OBSERVATION_ID:' || observation_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0) as procedure_concept_id
            , observation_date as procedure_date
            , observation_datetime as procedure_datetime
            , observation_type_concept_id as procedure_type_concept_id
            , cast( null as int ) as modifier_concept_id
            , cast( null as int ) as quantity
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , observation_source_value as procedure_source_value
            , observation_source_concept_id as procedure_source_concept_id
            , cast (null as string) as modifier_source_value
            , 'OBSERVATION' as source_domain
            , data_partner_id
            , payload
            from `ri.foundry.main.dataset.5f465bad-f62f-4fa7-832f-e8e8e653169b` o
            where o.observation_id is not null
            and o.target_domain_id = 'Procedure'

    ),
    /*
    ---rescued_from_the deprecated concepts
    rescued_concept_from_procedure as (
    ----- original data where the target domain is null find the target concept id using the relationship_ids in 
    ---- (concept poss_eq to, concept same_as to, concept replaced, RxNorm is a)
    --- and inserescued_concept_from_observationrt the rescued concepts from the deprecated concepts  
       select 
        u.site_domain_id
        , u.source_pkey || '|TARGET_CONCEPT_ID:' || COALESCE(xw.target_concept_id, '') as source_pkey
        , u.site_person_id
        , xw.target_concept_id as procedure_concept_id
        , p.procedure_date
        , p.procedure_datetime
        , p.procedure_type_concept_id
        , p.modifier_concept_id
        , p.quantity
        , p.provider_id as site_provider_id
        , p.visit_occurrence_id as site_visit_occurrence_id
        , p.visit_detail_id
        , p.procedure_source_value
        , xw.source_concept_id as procedure_source_concept_id
        , p.modifier_source_value
        , 'PROCEDURE' as source_domain
        , p.data_partner_id
        , p.payload
        from `ri.foundry.main.dataset.c647f4a5-1faa-4277-add5-8574c6f3e2e1` u
        inner join `ri.foundry.main.dataset.8e9c4a3f-516e-4288-8ec5-946c787ddb5a` p
        on p.procedure_occurrence_id = u.site_domain_id AND u.site_person_id = p.person_id AND p.procedure_concept_id = u.unmapped_source_concept_id 
         join `ri.foundry.main.dataset.504c654b-01bd-4285-9a98-961827d926ab` xw
        on u.unmapped_source_concept_id = xw.source_concept_id
        where xw.target_domain_id = 'Procedure'
    ),
    unmapped as (
        select 
        u.site_domain_id
        , u.source_pkey || '|TARGET_CONCEPT_ID:' || COALESCE(p.target_concept_id, '') as source_pkey
        , u.site_person_id
        ,  COALESCE(p.target_concept_id, p.procedure_concept_id ) as procedure_concept_id
        , p.procedure_date
        , p.procedure_datetime
        , p.procedure_type_concept_id
        , p.modifier_concept_id
        , p.quantity
        , p.provider_id as site_provider_id
        , p.visit_occurrence_id as site_visit_occurrence_id
        , p.visit_detail_id
        , p.procedure_source_value
        , p.procedure_source_concept_id
        , p.modifier_source_value
        , 'PROCEDURE' as source_domain
        , p.data_partner_id
        , p.payload
        from `ri.foundry.main.dataset.c647f4a5-1faa-4277-add5-8574c6f3e2e1` u
        inner join `ri.foundry.main.dataset.8e9c4a3f-516e-4288-8ec5-946c787ddb5a` p
        on p.procedure_occurrence_id = u.site_domain_id AND u.site_person_id = p.person_id AND p.procedure_concept_id = u.unmapped_source_concept_id 
        where u.unmapped_source_concept_id not in(
            select source_concept_id from `ri.foundry.main.dataset.504c654b-01bd-4285-9a98-961827d926ab` xw
            where target_domain_id = 'Procedure'

        ) ),
        */
    all_domain as (
        select *
         , md5(
             concat_ws( ';'
             , CAST(source_pkey as string)
             , COALESCE( CAST( procedure_concept_id as string), ''  )
             , COALESCE( CAST( procedure_source_concept_id as string), ''  )
             , COALESCE( source_domain, ''  )
         )) as hashed_id
         from (
             select * from procedure
             UNION
             select * from procedure_unmap
             union 
             select * from measurement
             union 
             select * from drug  --procedure from drug 
             union 
             select * from condition
             union
             select * from device
             UNION
             select * from observation
             --select * from unmapped
             --select * from rescued_concept_from_procedure
         )
    )

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as procedure_occurrence_id_51_bit
    FROM (
        SELECT * from all_domain
    )   