CREATE TABLE `ri.foundry.main.dataset.139dc174-3e09-4ed7-bec0-28d70d5b550a` AS

with patients_with_records AS (
    SELECT DISTINCT patid FROM (
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.0c17b2c1-862d-4103-9c4d-25507bcc13e4`		
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.905b16b4-cfff-44bf-bf5d-5dabf9a2ca76`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.781f51c6-3213-41a6-9764-292238ae9866`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.eafe63b1-42db-4f3f-ac2f-0b96f91c5260`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.5f3a9356-809c-465d-8725-f52dd918bd3c`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.710e7df0-575a-4b76-b799-bc0488f10e68`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.858a2339-3e51-4954-ba88-149584aab79d`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.634e56ef-35da-4cbb-9290-cf8950708c92`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.4b6a2c8d-b352-4574-9861-3bfc764b0900`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.3dacc398-2418-4bd9-9dac-4a67a1e000ad`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.b52c4215-d873-4931-a8a8-455282ec9a06`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.97e347ba-1a02-492f-8443-f0859a898389`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.7769b85a-98eb-473b-a414-843d7c7ffde3`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.da113963-4e92-42a2-9477-2591001cfcc1`
            UNION ALL
        SELECT DISTINCT patid FROM `ri.foundry.main.dataset.fbe7f6f9-c4de-4562-8f71-733706fbf168`
    )
),

demographic as (
    SELECT 
        demo.patid AS site_patid,
        cast(gx.TARGET_CONCEPT_ID as int) AS gender_concept_id,
        YEAR(birth_date) AS year_of_birth,
        MONTH(birth_date) AS month_of_birth,
        1 AS day_of_birth,
        cast(null as timestamp) as birth_datetime, --** MB: could use derived 'BIRTH_DATETIME' column, but times seem to all be 00:00
        -- rx.TARGET_CONCEPT_ID AS race_concept_id, 
        CASE 
            WHEN demo.race != '06' OR (demo.race = '06' AND demo.raw_race is null) 
                THEN cast(rx.TARGET_CONCEPT_ID as int)
            WHEN demo.race ='06' then cast(0 as int )  --SH:collective decision is made to map 06 other multi-race concept to 0 - no matching concept 
            ELSE cast(null as int)
        END AS race_concept_id,
        cast(ex.TARGET_CONCEPT_ID as int) AS ethnicity_concept_id, 
        -- lds.N3cds_Domain_Map_Id AS LOCATIONID, --** MB: this gets brought in through a join in step 6
        cast(null as long) as provider_id,
        cast(null as long) as care_site_id,
        cast(demo.patid as string) AS person_source_value, 
        cast(demo.sex as string) AS gender_source_value,  
        0 as gender_source_concept_id, 
        cast(demo.race as string) AS race_source_value, 
        0 AS race_source_concept_id,  
        cast(demo.hispanic as string) AS ethnicity_source_value, 
        0 AS ethnicity_source_concept_id, 
        'DEMOGRAPHIC' AS domain_source,
        data_partner_id,
        payload
    FROM `ri.foundry.main.dataset.ef9e40d3-9847-442c-98a6-80c6becfc6b9` demo  
        --** Remove patients with no records:
        INNER JOIN patients_with_records ON demo.patid = patients_with_records.patid
        LEFT JOIN `ri.foundry.main.dataset.7b494627-e6fb-4def-879b-a58a0f6d51d2` gx 
            ON gx.CDM_TBL = 'DEMOGRAPHIC'
            AND demo.sex = gx.SRC_GENDER 
        LEFT JOIN `ri.foundry.main.dataset.b0e5d144-3ceb-4ea1-9edd-843ef2bf5c8b` ex 
            ON ex.CDM_TBL = 'DEMOGRAPHIC' 
            AND demo.hispanic = ex.SRC_ETHNICITY 
        LEFT JOIN `ri.foundry.main.dataset.4212d6b2-b968-4a6d-905d-f4ccb38febab` rx 
            ON rx.CDM_TBL = 'DEMOGRAPHIC' 
            AND demo.race = rx.SRC_RACE 
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
                , md5(site_patid) as hashed_id
                FROM demographic
            )
        )
    )