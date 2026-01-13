CREATE TABLE `ri.foundry.main.dataset.67da37c5-7933-4c6c-8ef6-575e4d47a1aa` AS
    with visit_detail_domain as (
        SELECT DISTINCT
              vd.visit_detail_id as site_visit_detail_id
            ----, md5(concat_ws(COALESCE(CAST(visit_detail_id as string), ''), COALESCE(cast(visit_detail_concept_id as string), ''))) as hashed_id
            , vd.person_id as site_person_id
            , vd.visit_detail_concept_id
            , vd.visit_detail_start_date
            , vd.visit_detail_start_datetime
            , vd.visit_detail_end_date
            , vd.visit_detail_end_datetime
            , vd.visit_detail_type_concept_id
            , vd.provider_id as site_provider_id
            , vd.care_site_id as site_care_site_id
            , vd.visit_detail_source_value
            , vd.visit_detail_source_concept_id
            , vd.admitting_source_value
            , vd.admitting_source_concept_id
            , vd.discharge_to_source_value
            , vd.discharge_to_concept_id
            , vd.preceding_visit_detail_id as site_preceding_visit_detail_id
            , vd.visit_detail_parent_id
            , vd.visit_occurrence_id as site_visit_occurrence_id
            , vd.data_partner_id
            , vd.payload
        FROM `ri.foundry.main.dataset.63c67af0-f8f7-4ac7-9ae2-97d5a9817b2d` vd
        join `ri.foundry.main.dataset.afb8a46a-a052-4be9-ade5-1fc547ac2dde` v
         on vd.visit_occurrence_id = v.visit_occurrence_id
        join  `ri.foundry.main.dataset.163a9903-7852-4f09-a774-9d81f8672244` p
        on vd.person_id = p.person_id
        WHERE visit_detail_id IS NOT NULL
    ),
    final_table as (
        select *, 
         md5(concat_ws(';'
                , COALESCE(CAST(site_visit_detail_id as string), '')
                , COALESCE(cast(visit_detail_concept_id as string), '')
            )) as hashed_id
        from visit_detail_domain
    )

    SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as visit_detail_id_51_bit
    FROM final_table