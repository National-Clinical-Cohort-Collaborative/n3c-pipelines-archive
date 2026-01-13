CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 102/transform/04 - domain mapping/dqp_procedure_in_visit` AS
with procedure_temp as (
    SELECT distinct
        encounter_id AS site_encounter_id,
        patient_id AS site_patient_id,
        CAST(COALESCE(t.target_concept_id, 0) AS int) AS visit_concept_id,
        CAST(date AS date) AS visit_start_date,
        CAST(date AS timestamp) AS visit_start_datetime,
        CAST(null AS date) AS visit_end_date,
        CAST(null AS timestamp) AS visit_end_datetime,
        -- "For visit_type_concept _id -- SET default value = 32035 for  'Visit derived from EHR encounter record' "
        --CAST(32035 AS int) AS visit_type_concept_id,
        case when ( t.target_concept_id != 0 and t.target_concept_id is not null) then t.target_concept_id
            else 32035
            end as visit_type_concept_id,
        CAST(null AS long) AS provider_id,
        CAST(null AS long) AS care_site_id,
        CAST(null AS string) AS visit_source_value,
        CAST(null AS int) AS visit_source_concept_id,
        CAST(null AS int) AS admitting_source_concept_id,
        CAST(null AS string) AS admitting_source_value,
        CAST(null AS int) AS discharge_to_concept_id,
        CAST(null AS string) AS discharge_to_source_value,
        CAST(null AS long) AS preceding_visit_occurrence_id,
        CAST(data_partner_id as int) as data_partner_id,
        payload,
        'PROCEDURE' as source_domain
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 102/transform/03 - prepared/procedure` p
        left join `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 102/transform/t2o_code_xwalk_standard` t
            on upper(p.mapped_code_system) = upper(t.src_vocab_code)
            AND p.mapped_code= t.source_code
        where t.target_domain_id = 'Visit'
    

),
procedure as (
    SELECT data_partner_id, count(*) as procedure_with_visit_cnt
      from procedure_temp
    group by data_partner_id
),
  visit as (
      SELECT data_partner_id, count(*) as visit_incoming_cnt 
      from `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: TriNetX/raw_trinetx/Site 102/transform/04 - domain mapping/visit_occurrence`
      WHERE source_domain = 'PROCEDURE'
      group by data_partner_id
  )
    select p.*,v.visit_incoming_cnt from procedure p
left join visit v
on v.data_partner_id = p.data_partner_id