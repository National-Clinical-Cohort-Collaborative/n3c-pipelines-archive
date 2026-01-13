CREATE TABLE `ri.foundry.main.dataset.e5f71652-b192-48f9-aff4-b68c8ebd576a` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.a51a8aa3-bf84-4976-b41e-149cf2632f2c` d
    LEFT JOIN `ri.foundry.main.dataset.2d4e687c-2a97-4cca-9347-6f7df0c2d8ce` lookup
    ON d.visit_occurrence_id_51_bit = lookup.visit_occurrence_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as visit_occurrence_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(visit_occurrence_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
), 

global_visit as (

    SELECT
        global_id.*
        -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
        , p.person_id
        , care.care_site_id
        , prov.provider_id
    FROM global_id
    LEFT JOIN `ri.foundry.main.dataset.3b888e3f-a7f7-4251-a585-b0d0d987e29d` p
    ON global_id.site_person_id = p.site_person_id
    LEFT JOIN `ri.foundry.main.dataset.267c41ad-bc83-438b-8f99-ad573fc7a7f9` care
    ON global_id.site_care_site_id = care.site_care_site_id
    LEFT JOIN `ri.foundry.main.dataset.841cfefa-0485-48b7-a360-f359ab610eac` prov
    ON global_id.site_provider_id = prov.site_provider_id

)

SELECT gv.*
       ,preceding.visit_occurrence_id as preceding_visit_occurrence_id
FROM global_visit gv
LEFT JOIN global_visit preceding 
ON gv.site_preceding_visit_occurrence_id = preceding.site_visit_occurrence_id


  