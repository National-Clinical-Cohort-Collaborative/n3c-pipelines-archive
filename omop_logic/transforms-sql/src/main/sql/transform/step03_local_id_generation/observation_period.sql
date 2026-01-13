CREATE TABLE `ri.foundry.main.dataset.db7a68ca-7ba8-4bfb-ae11-49731d5ffd05` AS

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as observation_period_id_51_bit
    FROM (
        SELECT
              observation_period_id as site_observation_period_id
            , md5(CAST(observation_period_id as string)) as hashed_id
            , person_id as site_person_id
            , observation_period_start_date
            , observation_period_end_date
            , period_type_concept_id
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.55d4c948-f1cd-4e79-b9b4-2c4ff2020831`
        WHERE observation_period_id IS NOT NULL
    )   
