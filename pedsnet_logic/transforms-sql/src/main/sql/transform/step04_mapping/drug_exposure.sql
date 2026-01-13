CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/04 - mapping/drug_exposure` AS
  
with drugexp as (
     SELECT
              drug_exposure_id as source_domain_id
            , 'DURG_EXPOSURE_ID:' || drug_exposure_id || '|TARGET_CONCEPT_ID:' || COALESCE(d.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , COALESCE(d.target_concept_id, 0) as drug_concept_id 
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/drug_exposure` d
        WHERE drug_exposure_id IS NOT NULL
        -- Retain all records from the source table, unless we're already mapping them to another domain
        -- ie: If there are rows with domain_id == ('Device'), do not include them in this table
        ---e.g we are mapping the domain in Device, Procedure, Observation from Drug -  NOT IN ('Observation', 'Measurement', 'Device')
        AND d.target_concept_id IS NOT NULL
       AND (d.target_domain_id NOT IN ('Device', 'Observation', 'Measurement','Meas Value','Procedure','Condition','Visit') or d.target_domain_id is null)
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/drug_exposure` d
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

    procedures as (
        SELECT
            procedure_occurrence_id as source_domain_id
            , 'PROCEDURE_OCCURRENCE_ID:' || procedure_occurrence_id || '|TARGET_CONCEPT_ID:' || COALESCE(p.target_concept_id, '') as source_pkey
            , person_id as site_person_id
            , p.target_concept_id as drug_concept_id
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
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        AND p.target_domain_id = 'Drug'
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
            from `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/observation` o
            where o.observation_id is not null
            and o.target_domain_id ='Drug'
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
            from `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/device_exposure` d
            where d.device_exposure_id is not null
            and d.target_domain_id ='Drug'
    ), measurement as (
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
            from `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/measurement` m
            where m.measurement_id is not null
            and m.target_domain_id ='Drug'
    ),condition as (
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
            from `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/condition_occurrence` c
            where c.condition_occurrence_id is not null
            and c.target_domain_id = 'Drug'
    ),

  all_domain as ( 
        select distinct
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
             SELECT * FROM all_domain
         )