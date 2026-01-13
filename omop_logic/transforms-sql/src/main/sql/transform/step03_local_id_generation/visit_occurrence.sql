CREATE TABLE `ri.foundry.main.dataset.a51a8aa3-bf84-4976-b41e-149cf2632f2c` AS
    
    with visit as (
        SELECT
              visit_occurrence_id as site_visit_occurrence_id
              ,'VISIT_OCCURRENCE_ID:' || visit_occurrence_id  as source_pkey
            --, md5(CAST(visit_occurrence_id as string)) as hashed_id
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0 ) as visit_concept_id
            , visit_start_date
            , visit_start_datetime
            , visit_end_date
            , visit_end_datetime
            , visit_type_concept_id
            , provider_id as site_provider_id
            , care_site_id as site_care_site_id
            , visit_source_value
            , visit_source_concept_id
            , admitting_source_concept_id
            , admitting_source_value
            , discharge_to_concept_id
            , discharge_to_source_value
            , preceding_visit_occurrence_id as site_preceding_visit_occurrence_id
            , data_partner_id
            , payload
            ,'Visit' as source_domain
        FROM `ri.foundry.main.dataset.afb8a46a-a052-4be9-ade5-1fc547ac2dde`
        WHERE visit_occurrence_id IS NOT NULL
        and target_concept_id is not null
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Observation'), do not include them in this table, condition contained target domain id in ( condition, observation, null )
        AND target_domain_id NOT IN ('Device', 'Observation', 'Measurement','Procedure','Drug','Condition') 
    ), visit_null as (
        -- keep the unmapp concepts
        SELECT
              visit_occurrence_id as site_visit_occurrence_id
              ,'VISIT_OCCURRENCE_ID:' || visit_occurrence_id  as source_pkey
            --, md5(CAST(visit_occurrence_id as string)) as hashed_id
            , person_id as site_person_id
            , visit_concept_id
            , visit_start_date
            , visit_start_datetime
            , visit_end_date
            , visit_end_datetime
            , visit_type_concept_id
            , provider_id as site_provider_id
            , care_site_id as site_care_site_id
            , visit_source_value
            , visit_source_concept_id
            , admitting_source_concept_id
            , admitting_source_value
            , discharge_to_concept_id
            , discharge_to_source_value
            , preceding_visit_occurrence_id as site_preceding_visit_occurrence_id
            , data_partner_id
            , payload
            ,'Visit' as source_domain
        FROM `ri.foundry.main.dataset.afb8a46a-a052-4be9-ade5-1fc547ac2dde`
        WHERE visit_occurrence_id IS NOT NULL
        and target_concept_id is null


    ), procedure as (
        SELECT
              procedure_occurrence_id as site_visit_occurrence_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0) as visit_concept_id
            , procedure_date as visit_start_date
            , procedure_datetime as visit_start_datetime
            , procedure_date as visit_end_date
            , procedure_datetime as visit_end_datetime
            , procedure_type_concept_id as visit_type_concept_id
            , provider_id as site_provider_id
            ,cast ( null as long) as site_care_site_id
            , procedure_source_value as visit_source_value
            , procedure_source_concept_id as visit_source_concept_id
            , cast( null as int) as admitting_source_concept_id
            , cast( null as string) as admitting_source_value
            , cast( null as int) as discharge_to_concept_id
            , cast( null as string) as discharge_to_source_value
            ,cast(null as long) as site_preceding_visit_occurrence_id
            , data_partner_id
            , payload
            , 'PROCEDURE' as source_domain
        FROM `ri.foundry.main.dataset.8e9c4a3f-516e-4288-8ec5-946c787ddb5a` p
        WHERE procedure_occurrence_id IS NOT NULL
        and p.target_domain_id = 'Visit' and p.target_standard_concept = 'S'

    ),
    observation as (
        SELECT
              observation_id as site_visit_occurrence_id
            , 'OBSERVATION_ID:' || observation_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0) as visit_concept_id
            , observation_date as visit_start_date
            , observation_datetime as visit_start_datetime
            , observation_date as visit_end_date
            , observation_datetime as visit_end_datetime
            , observation_type_concept_id as visit_type_concept_id
            , provider_id as site_provider_id
            ,cast ( null as long) as site_care_site_id
            , observation_source_value as visit_source_value
            , observation_source_concept_id as visit_source_concept_id
            , cast( null as int) as admitting_source_concept_id
            , cast( null as string) as admitting_source_value
            , cast( null as int) as discharge_to_concept_id
            , cast( null as string) as discharge_to_source_value
            ,cast(null as long) as site_preceding_visit_occurrence_id
            , data_partner_id
            , payload
            , 'OBSERVATION' as source_domain
        FROM `ri.foundry.main.dataset.5f465bad-f62f-4fa7-832f-e8e8e653169b` o
        WHERE observation_id IS NOT NULL
        and o.target_domain_id = 'Visit' and o.target_standard_concept = 'S'
    ),

    drug as (
        SELECT
             drug_exposure_id as site_visit_occurrence_id
            , 'DRUG_EXPOSURE_ID:' || drug_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(target_concept_id, 0) as visit_concept_id
            , drug_exposure_start_date as visit_start_date
            , drug_exposure_start_datetime as visit_start_datetime
            , drug_exposure_end_date as visit_end_date
            , drug_exposure_end_datetime as visit_end_datetime
            , drug_type_concept_id as visit_type_concept_id
            , provider_id as site_provider_id
            ,cast ( null as long) as site_care_site_id
            , drug_source_value as visit_source_value
            , drug_source_concept_id as visit_source_concept_id
            , cast( null as int) as admitting_source_concept_id
            , cast( null as string) as admitting_source_value
            , cast( null as int) as discharge_to_concept_id
            , cast( null as string) as discharge_to_source_value
            ,cast(null as long) as site_preceding_visit_occurrence_id
            , data_partner_id
            , payload
            , 'DRUG' as source_domain
        FROM `ri.foundry.main.dataset.0e99bf0a-15ec-44db-b71d-8b711c8eab3a` d
        WHERE drug_exposure_id IS NOT NULL
        and d.target_domain_id = 'Visit' and d.target_standard_concept = 'S'
    ),
    visit_combine as (
        select * from visit union ALL
        select * from visit_null
    ),
    all_domain as (
        select *,md5(CAST(source_pkey as string)) as hashed_id from visit_combine
        union all 
        select *
         , md5(
             concat_ws( ';'
            , CAST(source_pkey as string)
             , COALESCE( CAST( visit_concept_id as string), ''  )
             , COALESCE( CAST( visit_source_concept_id as string), ''  )
             , COALESCE( source_domain, ''  )
         )) as hashed_id
         from (
             select * from procedure
            --  UNION ALL
            --  select * from visit
             union all 
             select * from observation
             union all
             select * from drug
             
  
         )
    )

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as visit_occurrence_id_51_bit
    FROM (
        all_domain
    )   
