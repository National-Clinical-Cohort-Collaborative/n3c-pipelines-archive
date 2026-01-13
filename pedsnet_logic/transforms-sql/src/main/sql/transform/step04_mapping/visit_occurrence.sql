CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/04 - mapping/visit_occurrence` AS
    

with visit as (
        SELECT
              cast(visit_occurrence_id as long) as site_visit_occurrence_id
            --, md5(CAST(visit_occurrence_id as string)) as hashed_id
            ,'VISIT_OCCURRENCE_ID:' || COALESCE(visit_occurrence_id,'')  as source_pkey
            , person_id as site_person_id
            , CAST(COALESCE(xwalk.TARGET_CONCEPT_ID, visit_concept_id) as int) as visit_concept_id
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
            , preceding_visit_occurrence_id as preceding_visit_occurrence_id
            , data_partner_id
            , payload
            ,'VISIT' as source_domain
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/visit_occurrence` visit
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Reference and Mapping Materials/pedsnet_visit_xwalk_table` xwalk
        ON visit.visit_concept_id = xwalk.SOURCE_ID
        WHERE visit_occurrence_id IS NOT NULL

),
procedure as (
        SELECT
              cast (procedure_occurrence_id as long ) as site_visit_occurrence_id
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
            ,cast(null as long) as preceding_visit_occurrence_id
            , data_partner_id
            , payload
            , 'PROCEDURE' as source_domain
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        and p.target_domain_id = 'Visit' and p.target_standard_concept = 'S'

),

    all_domain as (
        select distinct *, 
        md5(CAST(source_pkey as string)) as hashed_id
        from 
        visit

        union ALL

        select distinct *
         , md5(
             concat_ws( ';'
            , CAST(source_pkey as string)
             , COALESCE( CAST( visit_concept_id as string), ''  )
             , COALESCE( CAST( visit_source_concept_id as string), ''  )
             , COALESCE( source_domain, ''  )
             , COALESCE(site_person_id, ''  )
             , COALESCE(site_visit_occurrence_id, ''  )
             , COALESCE(visit_start_datetime, ''  )
             , COALESCE(visit_end_datetime, ''  )
             , COALESCE(visit_type_concept_id, ''  )
             , COALESCE(site_provider_id, ''  )
             , COALESCE(visit_source_value, ''  )
             , COALESCE(visit_source_concept_id, ''  )

         )) as hashed_id
         from
            procedure
    )

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as visit_occurrence_id_51_bit
    FROM (
        all_domain
    )   


    /*
    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as visit_occurrence_id_51_bit
    FROM (
        SELECT
              visit_occurrence_id as site_visit_occurrence_id
            , md5(CAST(visit_occurrence_id as string)) as hashed_id
            , person_id as site_person_id
            , CAST(COALESCE(xwalk.TARGET_CONCEPT_ID, visit_concept_id) as int) as visit_concept_id
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
            , preceding_visit_occurrence_id
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.a986d112-4eeb-4e08-b7d5-6dc12d504f59` visit
        LEFT JOIN `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Reference and Mapping Materials/pedsnet_visit_xwalk_table` xwalk
        ON visit.visit_concept_id = xwalk.SOURCE_ID
        WHERE visit_occurrence_id IS NOT NULL
    )   
*/