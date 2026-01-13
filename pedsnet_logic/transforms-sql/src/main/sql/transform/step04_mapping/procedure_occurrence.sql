CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/04 - mapping/procedure_occurrence` AS

-- procedure 2 procedure
    with procedures as (
        SELECT
            procedure_occurrence_id as source_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(p.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(p.target_concept_id, 0) as procedure_concept_id
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        and p.target_concept_id is not null
        -- Retain all records from the source table, unless we're already mapping them to another domain
        AND p.target_domain_id NOT IN ('Device', 'Drug', 'Meas Value','Measurement', 'Observation', 'Visit')
    ),
    procedure_unmap as (
        SELECT
            procedure_occurrence_id as source_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|PROCEDURE_CONCEPT_ID:' || COALESCE(procedure_concept_id, '') as source_pkey
            , person_id as site_person_id
            ,  procedure_concept_id
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/procedure_occurrence` p
        WHERE (procedure_occurrence_id IS NOT NULL
        -- concepts that are unmappable OR concepts that are mapped to visit domain but not a standard concept
        -- concepts that are mapped to visit domain will be moved to the visit_occurrence table
        and target_concept_id is null)
    ),
    condition as ( 
        SELECT
              condition_occurrence_id as source_domain_id
            , 'CONDITION_OCCURRENCE_ID:' || condition_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(c.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , target_concept_id as procedure_concept_id
            , condition_start_date as procedure_date
            , condition_start_datetime as procedure_datetime
            , 32817 as procedure_type_concept_id
            , CAST(NULL as int) modifier_concept_id
            , CAST(null as int) as quantity
            , provider_id as site_provider_id
            , visit_occurrence_id as site_visit_occurrence_id
            , visit_detail_id
            , condition_source_value as procedure_source_value
            , condition_source_concept_id as procedure_source_concept_id
            --, condition_status_source_value
            , CAST( NULL AS string) as modifier_source_value
            , 'CONDITION' as source_domain
            , data_partner_id
            , payload
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/condition_occurrence` c
        WHERE condition_occurrence_id IS NOT NULL
        AND c.target_domain_id = 'Procedure'
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
            from `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/observation` o
            where o.observation_id is not null
            and o.target_domain_id = 'Procedure'
    ),
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/measurement` m
        WHERE measurement_id IS NOT NULL 
        --AND m.target_concept_id IS NOT NULL
        AND m.target_domain_id = 'Procedure' 
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
            from `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/device_exposure` d
            where d.device_exposure_id is not null
            and d.target_domain_id = 'Procedure'

    ),
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/drug_exposure` d
        -- site are sending drug that may contain drug/null/device
        WHERE drug_exposure_id IS NOT NULL 
        --AND d.target_concept_id IS NOT NULL
        AND d.target_domain_id = 'Procedure'
    ),

    all_domain as ( 
         select distinct *
         , md5(
             concat_ws( ';'
             , CAST(source_pkey as string)
             , COALESCE( CAST( procedure_concept_id as string), ''  )
             , COALESCE( CAST( procedure_source_concept_id as string), ''  )
             , COALESCE( source_domain, ''  )
         )) as hashed_id
        from (
            select * from procedures 
                union all 
            select * from condition
            union all
            select * from procedure_unmap
            union all 
            select * from observation
            union all 
            select * from measurement
            union all 
            select * from device
            union all 
            select * from drug
        )
    ) 

    SELECT 
           * 
         , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as procedure_occurrence_id_51_bit
    FROM (
             SELECT * FROM all_domain
         )
