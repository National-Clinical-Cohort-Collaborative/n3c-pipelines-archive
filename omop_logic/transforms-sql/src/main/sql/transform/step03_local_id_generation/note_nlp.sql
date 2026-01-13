CREATE TABLE `ri.foundry.main.dataset.ed167d32-dd98-4eb9-b2aa-1c4fba7a822d` 
TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_MEDIUM') AS

    with final_table as (
    select distinct 
        note_nlp_id as site_note_nlp_id
            , note_id as site_note_id
            , n.section_concept_id
            , n.snippet
            , n.offset
            , n.lexical_variant
            , note_nlp_concept_id
            , note_nlp_source_concept_id
            , nlp_system
            , nlp_date	
            , nlp_datetime	
            , term_exists -- missing from schema
            , term_temporal 
            , term_modifiers
            , n.data_partner_id
            , n.payload
        FROM `ri.foundry.main.dataset.144e923a-22f7-453d-ac0e-6b6b8c2f43b6` n
        WHERE note_nlp_id IS NOT NULL 
    )

   SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as note_nlp_id_51_bit
    FROM (
        select *
            , md5( concat_ws(  
                COALESCE(CAST(site_note_nlp_id as string), '')
                , COALESCE(site_note_id, '')
                , COALESCE( note_nlp_concept_id, '')
                , COALESCE(offset, '')
                )) as hashed_id
        FROM final_table
    )  