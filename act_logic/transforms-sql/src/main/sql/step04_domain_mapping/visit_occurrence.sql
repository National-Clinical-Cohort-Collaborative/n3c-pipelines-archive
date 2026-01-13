CREATE TABLE `ri.foundry.main.dataset.3069c85a-1f5e-4046-bd47-8ebf8a9acd55` AS

with visit_dim as (
    SELECT 
        VISIT_DIMENSION_ID || '|' || 'Visit_dimension' || '|' || 32035 || '|' || COALESCE(vx.TARGET_CONCEPT_ID, 46237210) || '|' || CAST(enc.start_date as timestamp) as VISIT_DIMENSION_ID,
        encounter_num AS site_encounter_num,
        enc.patient_num AS site_patient_num,
        COALESCE(vx.TARGET_CONCEPT_ID, 46237210)  as visit_concept_id, -- 46237210 = "No information"
        CAST(enc.start_date as DATE) as visit_start_date,
        CAST(enc.start_date as timestamp) as visit_start_datetime,
        --** Use these values, or leave as null? 
        CAST(enc.end_date as date) as visit_end_date,
        CAST(enc.end_date as timestamp) as visit_end_datetime,
        -- confirmed this issue:
        ---Stephanie Hong 6/19/2020 -32035 -default to 32035 "Visit derived from EHR encounter record.
        ---case when enc.enc_type in ('ED', 'AV', 'IP', 'EI') then 38000251  -- need to check this with Charles / missing info
        ---when enc.enc_type in ('OT', 'OS', 'OA') then 38000269
        ---else 0 end AS VISIT_TYPE_CONCEPT_ID,  --check with SMEs
        32035 as visit_type_concept_id, ---- where did the record came from / need clarification from SME
        CAST(null AS long) as provider_id,
        CAST(null AS long) as care_site_id, -- CAN WE ADD LOCATION_CD IN visit_dimension as care_site? ssh 7/27/20
        CAST(enc.inout_cd as string) as visit_source_value,
        CAST(null AS int) AS visit_source_concept_id,
        CAST(null AS int) AS admitting_source_concept_id,
        CAST(null AS string) AS admitting_source_value,
        CAST(null AS int) AS discharge_to_concept_id,
        -- enc.dsch_disp_cd as DISCHARGE_TO_SOURCE_VALUE,
        CAST(null AS string) AS discharge_to_source_value,
        CAST(null AS long) AS preceding_visit_occurrence_id,
        'VISIT_DIMENSION' as domain_source,
        enc.data_partner_id,
        enc.payload
    FROM `ri.foundry.main.dataset.8466536f-cdf7-4818-8197-24a315bc5b52` enc
    JOIN `ri.foundry.main.dataset.898923b2-c392-4633-9016-c03748e49dad` p
    on enc.patient_num = p.patient_num and enc.data_partner_id = p.data_partner_id
        LEFT JOIN `ri.foundry.main.dataset.73caf530-65c3-4e6b-8b1f-1a5c6057c3d2` vx 
            ON vx.CDM_TBL = 'VISIT_DIMENSION' 
            AND vx.CDM_NAME='I2B2ACT' 
            AND vx.SRC_VISIT_TYPE = enc.inout_cd
),observation as (
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
  )

),
all_domain as (
  select * from visit_dim
  union all 
  select * from observation
)

SELECT
      *
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    , cast(base_10_hash_value as bigint) & 2251799813685247 as visit_occurrence_id_51_bit
    FROM (
        SELECT
          *
        , conv(sub_hash_value, 16, 10) as base_10_hash_value
        FROM (
            SELECT
              *
            , substr(hashed_id, 1, 15) as sub_hash_value
            FROM (
                SELECT
                  *
                -- Create primary key by hashing patient id to 128bit hexademical with md5,
                -- and converting to 51 bit int by first taking first 15 hexademical digits and converting
                --  to base 10 (60 bit) and then bit masking to extract the first 51 bits
                , md5(CAST(VISIT_DIMENSION_ID as string)) as hashed_id
                FROM all_domain
            )
        )
    )
