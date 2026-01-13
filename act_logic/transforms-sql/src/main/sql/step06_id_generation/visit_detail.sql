CREATE TABLE `ri.foundry.main.dataset.003baa77-27c1-4722-bcd9-85c8c782ca5b` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.789b02e5-4c62-4c54-b1e9-d72bcba3ea92` d
    LEFT JOIN `ri.foundry.main.dataset.34f5b33d-afe0-4923-9035-66eb02fde66b` lookup
    ON d.visit_detail_id_51_bit = lookup.visit_detail_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as visit_detail_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(visit_detail_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

    SELECT
        global_id.*
        -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
        , p.person_id
        , CAST( global_id.site_care_site_id AS long) as care_site_id
        , CAST( global_id.site_provider_id as long) as provider_id
        , visit.visit_occurrence_id
    FROM global_id
    LEFT JOIN `ri.foundry.main.dataset.ce0290bc-02a6-4fa7-883b-66c744f5a593` p
    ON global_id.site_person_id = p.site_patient_num
    LEFT JOIN `ri.foundry.main.dataset.f7fc5c89-ff9c-4399-88f7-1f806a9bc01b` visit
    ON global_id.site_visit_occurrence_id = visit.site_encounter_num
    where visit.domain_source != 'OBSERVATION_FACT'