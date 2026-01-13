CREATE TABLE `ri.foundry.main.dataset.8ee9d6c1-a876-4756-93d5-af3d644eac31` AS

with patients_with_records AS (
    SELECT DISTINCT patient_num FROM (
        SELECT DISTINCT patient_num FROM `ri.foundry.main.dataset.8466536f-cdf7-4818-8197-24a315bc5b52`		
            UNION ALL
        SELECT DISTINCT patient_num FROM `ri.foundry.main.dataset.b45684f1-ba10-4a41-a49b-34a80dee15c4`
    )
),

pat_dim as (
    SELECT
        pd.patient_num AS site_patient_num,
        CAST(COALESCE(gender_xwalk.TARGET_CONCEPT_ID, 0) as int) AS gender_concept_id,
        YEAR(birth_date) AS year_of_birth,
        MONTH(birth_date) AS month_of_birth,
        DAYOFMONTH(birth_date) AS day_of_birth, 
        -- 1 AS day_of_birth,
        CAST(null as timestamp) AS birth_datetime,
        CAST(COALESCE(race_xwalk.TARGET_CONCEPT_ID, 0) as int) AS race_concept_id, 
        --CASE WHEN pd.RACE != '06' OR (demo.RACE='06' AND demo.raw_race is null) then race_xwalk.TARGET_CONCEPT_ID
        --    ELSE null
        --    END AS race_concept_id,
        CAST(COALESCE(ethnicity_xwalk.TARGET_CONCEPT_ID, 0) as int) AS ethnicity_concept_id,
        loc.hashed_id as location_hashed_id, --** Use later to join on location table for global id
        CAST(null as long) as provider_id,
        CAST(null as long) as care_site_id,
        CAST(pd.patient_num as string) AS person_source_value,
        CAST(COALESCE(pd.sex_cd, sex.concept_cd) as string) AS gender_source_value,
        0 AS gender_source_concept_id,
        CAST(COALESCE(pd.race_cd, race.concept_cd) as string) AS race_source_value,
        0 AS race_source_concept_id,
        CAST(COALESCE(pd.ethnicity_cd, ethnicity.concept_cd) as string) AS ethnicity_source_value,
        0 AS ethnicity_source_concept_id,
        'PATIENT_DIMENSION' AS domain_source,
        pd.data_partner_id,
        pd.payload
    FROM `ri.foundry.main.dataset.898923b2-c392-4633-9016-c03748e49dad` pd
        --** Remove patients with no records:
        INNER JOIN patients_with_records ON pd.patient_num = patients_with_records.patient_num
        LEFT JOIN (
                    select patient_num, concept_cd, ROW_NUMBER() OVER (PARTITION BY patient_num ORDER BY start_date DESC) row_num
                        from `ri.foundry.main.dataset.b45684f1-ba10-4a41-a49b-34a80dee15c4` ethnicity_ob 
                        where upper(concept_cd) like 'DEM|HISP%'
                    ) ethnicity 
                    ON ethnicity.patient_num = pd.patient_num 
                    and ethnicity.row_num = 1
        LEFT JOIN (
                    select patient_num, concept_cd, ROW_NUMBER() OVER (PARTITION BY patient_num ORDER BY start_date DESC) row_num
                    from `ri.foundry.main.dataset.b45684f1-ba10-4a41-a49b-34a80dee15c4` race_ob 
                    where upper(concept_cd) like 'DEM|RACE%'
                    ) race 
                    ON race.patient_num = pd.patient_num 
                    and race.row_num = 1
        LEFT JOIN (
                    select patient_num, concept_cd, ROW_NUMBER() OVER ( PARTITION BY patient_num ORDER BY start_date DESC) row_num
                    from `ri.foundry.main.dataset.b45684f1-ba10-4a41-a49b-34a80dee15c4` sex_ob 
                    where upper(concept_cd) like 'DEM|SEX%'
                    ) sex 
                    ON sex.patient_num = pd.patient_num 
                    and sex.row_num = 1
        LEFT JOIN `ri.foundry.main.dataset.462464b9-d54c-45bd-9331-38a3a7c391d1` gender_xwalk 
            ON gender_xwalk.CDM_NAME = 'I2B2ACT'
            AND gender_xwalk.CDM_TBL = 'PATIENT_DIMENSION'
            AND gender_xwalk.SRC_GENDER = COALESCE(pd.sex_cd, sex.concept_cd)
        LEFT JOIN `ri.foundry.main.dataset.a613a98f-66e3-4308-a5da-f39304d66ba6` ethnicity_xwalk 
            ON ethnicity_xwalk.CDM_NAME = 'I2B2ACT'
            AND ethnicity_xwalk.CDM_TBL = 'PATIENT_DIMENSION'
            AND ethnicity_xwalk.SRC_ETHNICITY = COALESCE(pd.ethnicity_cd, ethnicity.concept_cd)
        LEFT JOIN `ri.foundry.main.dataset.f477f04c-edd5-407e-905e-a64e6c3f0bdb` race_xwalk 
            ON race_xwalk.CDM_NAME = 'I2B2ACT'
            AND race_xwalk.CDM_TBL = 'PATIENT_DIMENSION'
            AND race_xwalk.SRC_RACE = COALESCE(pd.race_cd, race.concept_cd)
        LEFT JOIN `ri.foundry.main.dataset.7fb74da4-8574-4942-8592-34614b3d7735` loc 
            ON loc.zip = pd.zip_cd
)

SELECT
      *
    -- 2251799813685247 = ((1 << 51) - 1) - bitwise AND gives you the first 51 bits
    , cast(base_10_hash_value as bigint) & 2251799813685247 as person_id_51_bit
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
                , md5(CAST(site_patient_num as string)) as hashed_id
                FROM pat_dim
            )
        )
    )