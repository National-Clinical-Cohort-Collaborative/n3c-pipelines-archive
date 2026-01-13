CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/04 - mapping/observation` AS  

    with observation as (
        SELECT
               observation_id as source_domain_id
             , 'OBSERVATION_ID:' || observation_id || '|TARGET_CONCEPT_ID:' || COALESCE(o.target_concept_id, '') as source_pkey
             , person_id as site_person_id
             , COALESCE(o.target_concept_id, 0) as observation_concept_id
             , observation_date
             , observation_datetime
             , 32817 as observation_type_concept_id
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
             , 'OBSERVATION' as source_domain
             , data_partner_id
             , payload
         FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/observation` o
         WHERE observation_id IS NOT NULL
         and o.target_concept_id is not null
         -- Retain all records from the source table, unless we're already mapping them to another domain
         AND ( o.target_domain_id NOT IN ('Device', 'Condition','Meas Value',  'Measurement','Procedure','Drug','Visit'))
    ), gender as (
        SELECT
        condition_occurrence_id as source_domain_id
        , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
        , person_id as site_person_id
        , target_concept_id as observation_concept_id
        , condition_start_date as observation_date
        , condition_start_datetime as observation_datetime
        , 32817 as observation_type_concept_id --ehr origin
        , CAST(null as int ) as value_as_number
        , CAST(null as string) value_as_string
        , condition_status_concept_id as value_as_concept_id -- condition status concept id like final diagnosis
        , CAST(NULL as int) as qualifier_concept_id
        , CAST(NULL as int) as unit_concept_id
        , provider_id as site_provider_id
        , visit_occurrence_id as site_visit_occurrence_id
        , visit_detail_id
        , condition_source_value as observation_source_value
        , condition_source_concept_id as observation_source_concept_id
        , CAST( NULL AS string) as unit_source_value
        , CAST( NULL AS string) as qualifier_source_value
        , 'GENDER' as source_domain
        , data_partner_id
        , payload
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/condition_occurrence` c
    WHERE condition_occurrence_id IS NOT NULL
    AND c.target_domain_id = 'Gender'
    ),

    condition as (      
        SELECT
              condition_occurrence_id as source_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , target_concept_id as observation_concept_id
            , condition_start_date as observation_date
            , condition_start_datetime as observation_datetime
            , 32817 as observation_type_concept_id --ehr origin
            , CAST(null as int ) as value_as_number
            , CAST(null as string) value_as_string
            , condition_status_concept_id as value_as_concept_id -- condition status concept id like final diagnosis
            , CAST(NULL as int) as qualifier_concept_id
            , CAST(NULL as int) as unit_concept_id
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as observation_source_value
            , condition_source_concept_id as observation_source_concept_id
            , CAST( NULL AS string) as unit_source_value
            , CAST( NULL AS string) as qualifier_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/condition_occurrence` c
        WHERE condition_occurrence_id IS NOT NULL
        AND c.target_domain_id = 'Observation'
    ),

    procedures as ( 
        SELECT
            procedure_occurrence_id as source_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(p.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , target_concept_id as observation_concept_id
            , procedure_date as observation_date
            , procedure_datetime as observation_datetime
            , 32817 as observation_type_concept_id
            , CAST(null as int) value_as_number
            , CAST(null as string) value_as_string
            , procedure_concept_id as value_as_concept_id
            , CAST(NULL as int) as qualifier_concept_id
            , CAST(NULL as int) as unit_concept_id
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        AND p.target_domain_id = 'Observation'
    ),

    measurement as (
        SELECT
              measurement_id as source_domain_id
            , 'MEASUREMENT_ID:' || measurement_id || '|TARGET_CONCEPT_ID:' || COALESCE(m.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , m.target_concept_id as observation_concept_id
            , measurement_date as observation_date
            , measurement_datetime as observation_datetime
            --, measurement_time
            , 32817 as observation_type_concept_id
            , m.value_as_number
            , CAST( NULL as string ) as value_as_string
            , m.value_as_concept_id
            , m.operator_concept_id as qualifier_concept_id
            , unit_concept_id
            --, range_low
            --, range_high
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , measurement_source_value as observation_source_value
            , measurement_source_concept_id as observation_source_concept_id
            , m.unit_source_value
            , CAST(NULL AS string) as qualifier_source_value
            , 'MEASUREMENT' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/measurement` m
        WHERE measurement_id IS NOT NULL
        AND m.target_domain_id = 'Observation' 
 
    ), observation_unmap as (
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
    FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/observation` o
    WHERE (observation_id IS NOT NULL 
    and target_concept_id is null) 
    ),device as (
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
        from  `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/device_exposure` d
        where d.device_exposure_id is not null
        and d.target_domain_id = 'Observation'
    ), drug as (
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
        FROM `ri.foundry.main.dataset.293d0212-2f37-4058-a2e2-f075b69c681d` dr
        WHERE drug_exposure_id IS NOT NULL
        --AND d.target_concept_id IS NOT NULL -- deprecated concept_id will result in the null target_concept_id
        And dr.target_domain_id = 'Observation'
    ),
 all_domain as (
        select distinct * from observation
        union
        select * from observation_unmap
        union
        select * from gender
        union
        select distinct * from condition
        union 
        select distinct * from procedures
        UNION
        select distinct * from drug
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