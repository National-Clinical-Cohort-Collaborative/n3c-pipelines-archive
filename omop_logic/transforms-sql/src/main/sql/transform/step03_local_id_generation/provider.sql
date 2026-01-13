CREATE TABLE `ri.foundry.main.dataset.5025baee-cb2f-4d99-a262-0d159dea593f` AS

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as provider_id_51_bit
    FROM (
        SELECT
              provider_id as site_provider_id
            , md5(CAST(provider_id as string)) as hashed_id
            , provider_name
            , npi
            , dea
            , specialty_concept_id
            , care_site_id as site_care_site_id
            , year_of_birth
            , gender_concept_id
            , provider_source_value
            , specialty_source_value
            , specialty_source_concept_id
            , gender_source_value
            , gender_source_concept_id
            , data_partner_id
            , payload
        FROM `ri.foundry.main.dataset.6adaa641-cff0-4628-984d-518b0d712f20`
        WHERE provider_id IS NOT NULL
    )   