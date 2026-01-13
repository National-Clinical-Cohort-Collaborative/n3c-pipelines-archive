CREATE TABLE `ri.foundry.main.dataset.09380fbf-ab78-4930-ab18-b492bb5cb968` AS

 --- ## TODO -- there may be note data from observation -- when this is added, move it out of the observation by only adding data from domain_id=Observation
   SELECT 
          * 
        , cast(conv(substr(hashed_id, 1, 15), 16, 10) as bigint) & 2251799813685247 as note_id_51_bit
    FROM (
        SELECT
           n.note_id as site_note_id
            , md5(CAST(note_id as string)) as hashed_id
            , n.person_id as site_person_id
            , n.note_date
            , n.note_datetime
            , n.note_type_concept_id
            , n.note_class_concept_id
            , n.note_title 
            , n.note_text
            , n.encoding_concept_id
            , n.language_concept_id
            , n.provider_id as site_provider_id
            , n.visit_occurrence_id as site_visit_occurrence_id
            , n.visit_detail_id as site_visit_detail_id
            , n.note_source_value
            , n.data_partner_id
            , n.payload
        FROM `ri.foundry.main.dataset.49b2f81e-ca33-4b52-9ebd-aa30a8f8fb30` n
        WHERE note_id IS NOT NULL
    )