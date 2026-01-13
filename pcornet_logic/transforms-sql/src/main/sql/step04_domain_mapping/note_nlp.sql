CREATE TABLE `ri.foundry.main.dataset.830300aa-2728-438d-904a-4a228d3e569f` AS

   SELECT
          *
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as note_nlp_id_51_bit
    FROM (
        SELECT
           n.note_nlp_id as site_note_nlp_id
            , md5(CAST(note_nlp_id as string)) as hashed_id
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
            , CAST(null as boolean) as term_exists
            , term_temporal 
            , term_modifiers
            , CAST(n.data_partner_id as int) as data_partner_id
            , n.payload
        FROM `ri.foundry.main.dataset.4296ccff-9296-431e-8f1d-63e6470e6ec5` n
        WHERE note_id IS NOT NULL
    )  