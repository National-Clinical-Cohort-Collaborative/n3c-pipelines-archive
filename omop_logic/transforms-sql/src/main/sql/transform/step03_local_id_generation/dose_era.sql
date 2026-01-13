CREATE TABLE `ri.foundry.main.dataset.5af76cfd-5869-48ea-8d0d-080cb9afae20` AS

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as dose_era_id_51_bit
    FROM (
        SELECT
              dose_era_id as site_dose_era_id
            , md5(CAST(dose_era_id as string)) as hashed_id
            , person_id	as site_person_id
            , COALESCE(target_concept_id, drug_concept_id) as drug_concept_id	
            , unit_concept_id	
            , dose_value	
            , dose_era_start_date	
            , dose_era_end_date	
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.7df84794-0e89-4831-b9e9-133d23ae5b49`
        WHERE dose_era_id IS NOT NULL
    )   
