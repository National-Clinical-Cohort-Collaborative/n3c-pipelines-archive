CREATE TABLE `ri.foundry.main.dataset.39bcd55d-24d4-49c0-bc32-2d02c8f7a4bb` AS 

with pat_dim_and_obs_fact as (
    -- Language observations from PATIENT_DIMENSION table
    SELECT
        pd.patient_num as site_patient_num,
        4152283 AS observation_concept_id, --** "Main spoken language"
        CAST(ee.obs_date as date) AS observation_date,
        CAST(ee.obs_date as timestamp) AS observation_datetime,
        0 AS observation_type_concept_id,
        0 value_as_number,
        CAST(pd.language_cd as string) AS value_as_string,
        CAST(lang.TARGET_CONCEPT_ID as int) AS value_as_concept_id,
        0 AS qualifier_concept_id,
        0 AS unit_concept_id,
        CAST(null as long) AS provider_id,
        CAST(null as int) AS site_encounter_num,
        CAST(null as long) AS visit_detail_id,
        'src=I2B2ACT_PATIENT_DIM dt=earliest ENC for Person' AS observation_source_value,
        0 observation_source_concept_id, ---preferred language source concept id 45882691
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'PATIENT_DIMENSION' domain_source,
        pd.patient_num as site_comparison_key,
        data_partner_id,
        payload
    FROM `ri.foundry.main.dataset.898923b2-c392-4633-9016-c03748e49dad` pd
        INNER JOIN `ri.foundry.main.dataset.965c112a-83cb-44b6-b36c-e1ba7730048c` lang 
            ON lang.CDM_TBL_COLUMN_NAME = 'LANGUAGE_CD'
            AND lang.CDM_SOURCE = 'I2B2ACT'
            AND lang.CDM_TBL = 'PATIENT_DIMENSION'
            AND lang.SRC_CODE = pd.language_cd
        LEFT JOIN (
            SELECT
                patient_num,
                MIN(start_date) AS obs_date
            FROM
                `ri.foundry.main.dataset.8466536f-cdf7-4818-8197-24a315bc5b52`
            GROUP BY
                patient_num
        ) ee ON ee.patient_num = pd.patient_num

UNION ALL

    -- OBSERVATION_FACT to observation
    ---- SDoH answer codes are coming in via the valueflag_cd column, 
    ---- the SDoH answer code mapping will not be found in the valueset mapping table
    ---- join with the generated sdoh_answer_code_xwalk table to get the LOINC coded answer code's concept_id
    SELECT DISTINCT
        ob.patient_num as site_patient_num,
        CAST(xw.target_concept_id as int) AS observation_concept_id, 
        CAST(ob.start_date as date) AS observation_date,
        CAST(ob.start_date as timestamp) AS observation_datetime,
        32817 AS observation_type_concept_id, --32817 EHR type concept id, updated 4/11/22, shong
        CASE
            WHEN valtype_cd = 'N' THEN
                CAST(ob.nval_num as float)
            ELSE
                -- valtype_cd = 'T'
                CAST(null as float)
        END AS value_as_number,
        CASE
            WHEN valtype_cd = 'T' THEN
                CAST(ob.tval_char as string)
            ELSE 
                -- valtype_cd = 'N'
                CAST(null as string)
        END AS value_as_string,
        -- if vfqual.target_concept_id is null then try to use the sdoh concept answer_code_target_concept_id
        -- if the valueflag_cd is LOINC coded SDoH answer concept code than the vfqual.target_concept_id will be null
        CAST(COALESCE(vfqual.TARGET_CONCEPT_ID, anxw.target_concept_id, 0) as int) AS value_as_concept_id,
        -- 0 AS qualifier_concept_id, -- tval_char will contain operator code if valtype_cd is N for numeric
        CASE
            WHEN valtype_cd = 'N'
                    AND tval_char = 'E' THEN
                4172703 ----4319898
            WHEN valtype_cd = 'N'
                    AND tval_char = 'G' THEN
                4172704 ---4139823
            WHEN valtype_cd = 'N'
                    AND tval_char = 'L' THEN
                4171756
            WHEN valtype_cd = 'N'
                    AND tval_char = 'LE' THEN
                4171754
            WHEN valtype_cd = 'N'
                    AND tval_char = 'GE' THEN
                4171755
            ELSE
                0
        END AS qualifier_concept_id, -- tval_char will contain operator code if valtype_cd is N for numeric
        CASE
            WHEN valtype_cd = 'T' THEN
                0
            ELSE
                CAST(units.TARGET_CONCEPT_ID as int)
        END AS unit_concept_id,
        CAST(null as long) AS provider_id,
        ob.encounter_num AS site_encounter_num,
        CAST(null as long) AS visit_detail_id,
        CAST(COALESCE(ob.mapped_concept_cd, ob.concept_cd) as string) AS observation_source_value,
        CAST(xw.source_concept_id as int) AS observation_source_concept_id,
        CAST(units_cd as string) AS unit_source_value,
        CAST(valueflag_cd as string) AS qualifier_source_value,
        'OBSERVATION_FACT' domain_source,
        site_comparison_key,
        data_partner_id,
        payload
    FROM `ri.foundry.main.dataset.b45684f1-ba10-4a41-a49b-34a80dee15c4` ob
        INNER JOIN `ri.foundry.main.dataset.4691f539-2a72-4abe-a1fa-d2f6a9cba939` xw 
            ON xw.src_code_type || ':' || xw.src_code = COALESCE(ob.mapped_concept_cd, ob.concept_cd)
            AND xw.cdm_tbl = 'OBSERVATION_FACT'
            AND xw.target_domain_id = 'Observation'
        LEFT JOIN `ri.foundry.main.dataset.965c112a-83cb-44b6-b36c-e1ba7730048c` units 
            ON ob.units_cd = units.SRC_CODE
            AND units.CDM_TBL_COLUMN_NAME = 'UNITS_CD'
        LEFT JOIN `ri.foundry.main.dataset.965c112a-83cb-44b6-b36c-e1ba7730048c` vfqual 
            ON lower(TRIM(ob.valueflag_cd)) = lower(TRIM(vfqual.SRC_CODE))
            AND vfqual.CDM_TBL = 'OBSERVATION_FACT'
            AND vfqual.CDM_TBL_COLUMN_NAME = 'VALUEFLAG_CD'
        LEFT JOIN  `ri.foundry.main.dataset.8e640377-1570-4aec-bf87-c4855ff418bf`  anxw 
        on answer_code = trim(substring(ob.valueflag_cd, 7, length(ob.valueflag_cd)-6))
        AND anxw.CDM_TBL = 'OBSERVATION_FACT'   
        -- We are processing the longcovid below and ADTEvents in visit detail so filter them out from this query. shong 5/12/2022
        WHERE UPPER(ob.concept_cd) NOT LIKE 'N3C:%' -- all long codvid and adtevents will have N3C: prefix
        AND UPPER(ob.concept_cd) NOT LIKE 'UMLS:%' AND UPPER(ob.concept_cd) NOT LIKE 'CPT4:%' AND UPPER(ob.concept_cd) NOT LIKE 'NDC:%'
        AND UPPER(ob.concept_cd) NOT LIKE 'RXNORM:%'
UNION ALL
---- addition of long covid data to observation from observation_fact table
---- long covid data according to the spec should contain concept_cd =N3C:long-covid clinic prefix
---- long covid specialty clinic visit data to observation
    SELECT DISTINCT
        ob.patient_num as site_patient_num,
        2004207791 AS observation_concept_id, -----2004207791=N3C observation concept id for long COVID clinic visit 
        CAST(ob.start_date as date) AS observation_date,
        CAST(ob.start_date as timestamp) AS observation_datetime,
        32817 AS observation_type_concept_id,
        CAST(null as float) AS value_as_number,
        CAST(null as string) AS value_as_string,
        CAST(null as int) AS value_as_concept_id,
        CAST(null as int) AS qualifier_concept_id,
        CAST(null as int) AS unit_concept_id,
        CAST(null as long) AS provider_id,
        ob.encounter_num AS site_encounter_num,
        CAST(null as long) AS visit_detail_id,
        CAST(COALESCE(ob.mapped_concept_cd, ob.concept_cd) as string) AS observation_source_value,
        2004207791 AS observation_source_concept_id,
        CAST(null as string) AS unit_source_value,
        CAST(null as string) AS qualifier_source_value,
        'OBSERVATION_FACT' domain_source,
        site_comparison_key,
        data_partner_id,
        payload
    FROM `ri.foundry.main.dataset.b45684f1-ba10-4a41-a49b-34a80dee15c4` ob
    WHERE concept_cd like 'N3C:long-covid clinic%'

), 


final_table AS (
-- Finding only unique measurements, per discussion with Matvey Palchuk + Kristin Kosta 13-08-2020
    SELECT DISTINCT *
    FROM pat_dim_and_obs_fact
    WHERE observation_concept_id IS NOT NULL
)

SELECT
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    -- In case of collisions, this will be joined on a lookup table in the next step
      cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_id_51_bit
    -- Pass through the hashed id to join on lookup table in case of conflicts
    , hashed_id
    , site_patient_num
    , observation_concept_id
    , observation_date
    , observation_datetime
    , observation_type_concept_id
    , value_as_number
    , value_as_string
    , value_as_concept_id
    , qualifier_concept_id
    , unit_concept_id
    , provider_id
    , site_encounter_num
    , visit_detail_id
    , observation_source_value
    , observation_source_concept_id
    , unit_source_value
    , qualifier_source_value
    , domain_source
    , site_comparison_key
    , data_partner_id
    , payload
FROM (
    SELECT
          *
        , md5(concat_ws(
              ';'
			, COALESCE(site_patient_num, ' ')
			, COALESCE(observation_concept_id, ' ')
			, COALESCE(observation_date, ' ')
			, COALESCE(observation_datetime, ' ')
			, COALESCE(observation_type_concept_id, ' ')
			, COALESCE(value_as_number, ' ')
			, COALESCE(value_as_string, ' ')
			, COALESCE(value_as_concept_id, ' ')
			, COALESCE(qualifier_concept_id, ' ')
			, COALESCE(unit_concept_id, ' ')
			, COALESCE(provider_id, ' ')
			, COALESCE(site_encounter_num, ' ')
			, COALESCE(visit_detail_id, ' ')
			, COALESCE(observation_source_value, ' ')
			, COALESCE(observation_source_concept_id, ' ')
			, COALESCE(unit_source_value, ' ')
			, COALESCE(qualifier_source_value, ' ')
			, COALESCE(site_comparison_key, ' ')
        )) as hashed_id
    FROM final_table
)
