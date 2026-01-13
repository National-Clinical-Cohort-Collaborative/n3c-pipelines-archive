CREATE TABLE `ri.foundry.main.dataset.ac5cba26-3e69-4aed-bf27-9a5a9f3da074` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.67da37c5-7933-4c6c-8ef6-575e4d47a1aa` d
    LEFT JOIN `ri.foundry.main.dataset.dc33f237-da0b-43f4-be27-dcc68b8fb0b6` lookup
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
),

global_visit_detail as ( 

    SELECT
        global_id.*
        -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
        , p.person_id
        , care.care_site_id
        , prov.provider_id
        , visit.visit_occurrence_id
    FROM global_id
    LEFT JOIN `ri.foundry.main.dataset.3b888e3f-a7f7-4251-a585-b0d0d987e29d` p
    ON global_id.site_person_id = p.site_person_id
    LEFT JOIN `ri.foundry.main.dataset.267c41ad-bc83-438b-8f99-ad573fc7a7f9` care
    ON global_id.site_care_site_id = care.site_care_site_id
    LEFT JOIN `ri.foundry.main.dataset.841cfefa-0485-48b7-a360-f359ab610eac` prov
    ON global_id.site_provider_id = prov.site_provider_id
    LEFT JOIN `ri.foundry.main.dataset.e5f71652-b192-48f9-aff4-b68c8ebd576a` visit
    ON global_id.site_visit_occurrence_id = visit.site_visit_occurrence_id
)

SELECT gvd.*
    , enc.visit_occurrence_id as preceding_visit_detail_id
    FROM global_visit_detail gvd
    LEFT JOIN `ri.foundry.main.dataset.e5f71652-b192-48f9-aff4-b68c8ebd576a` enc
    ON gvd.site_preceding_visit_detail_id = enc.site_visit_occurrence_id