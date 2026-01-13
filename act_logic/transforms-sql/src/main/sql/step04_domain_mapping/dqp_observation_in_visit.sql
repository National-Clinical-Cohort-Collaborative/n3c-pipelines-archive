CREATE TABLE `ri.foundry.main.dataset.af624bd4-7503-48dc-b608-9b1fb5446e27` AS
    with ob_temp(
        select site_encounter_num ||'|'|| site_patient_num || '|' || 'Observation_fact' || '|' || visit_type_concept_id || '|' || visit_concept_id || '|' || visit_start_datetime as VISIT_DIMENSION_ID,
  *
  from (
  SELECT 
  distinct
        
          encounter_num AS site_encounter_num,
          o.patient_num AS site_patient_num,
          COALESCE(xw.target_concept_id, 46237210)  as visit_concept_id, -- 46237210 = "No information"
          CAST(o.start_date as DATE) as visit_start_date,
          CAST(o.start_date as timestamp) as visit_start_datetime,
          --** Use these values, or leave as null? 
          CAST(o.end_date as date) as visit_end_date,
          CAST(o.end_date as timestamp) as visit_end_datetime,
          case when ( xw.target_concept_id != 0 and xw.target_concept_id is not null) then xw.target_concept_id
              else 32035
              end as visit_type_concept_id,
          CAST(null AS long) as provider_id,
          CAST(null AS long) as care_site_id, -- CAN WE ADD LOCATION_CD IN visit_dimension as care_site? ssh 7/27/20
          CAST(null as string) as visit_source_value,
          CAST(null AS int) AS visit_source_concept_id,
          CAST(null AS int) AS admitting_source_concept_id,
          CAST(null AS string) AS admitting_source_value,
          CAST(null AS int) AS discharge_to_concept_id,
          -- enc.dsch_disp_cd as DISCHARGE_TO_SOURCE_VALUE,
          CAST(null AS string) AS discharge_to_source_value,
          CAST(null AS long) AS preceding_visit_occurrence_id,
          'OBSERVATION_FACT' as domain_source,
          o.data_partner_id,
          o.payload
          FROM `ri.foundry.main.dataset.b45684f1-ba10-4a41-a49b-34a80dee15c4` o
          INNER JOIN `ri.foundry.main.dataset.4691f539-2a72-4abe-a1fa-d2f6a9cba939` xw 
              ON xw.src_code_type || ':' || xw.src_code = COALESCE(o.mapped_concept_cd, o.concept_cd)
              -- ON xw.source_code = substr(o.concept_cd, instr(o.concept_cd, ':')+1, length(o.concept_cd))
              AND xw.cdm_tbl = 'OBSERVATION_FACT'
              AND xw.target_domain_id = 'Visit'
  )),
  ob as (
      SELECT data_partner_id, count(*) as observation_with_visit_cnt
      from ob_temp
    group by data_partner_id
  ),
  visit as (
      SELECT data_partner_id, count(*) as visit_incoming_cnt 
      from `ri.foundry.main.dataset.3069c85a-1f5e-4046-bd47-8ebf8a9acd55`
      WHERE domain_source = 'OBSERVATION_FACT'
      group by data_partner_id
  )
    select o.*,v.visit_incoming_cnt from ob o
left join visit v
on o.data_partner_id = o.data_partner_id