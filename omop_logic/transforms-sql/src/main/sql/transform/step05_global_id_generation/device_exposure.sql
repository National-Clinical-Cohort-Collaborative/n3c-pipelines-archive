CREATE TABLE `ri.foundry.main.dataset.d2055022-3ee2-4bdd-bfc3-2f4794309b49` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.813f1ad9-71b0-43ba-8692-5de0bb3b66d3` d
    LEFT JOIN `ri.foundry.main.dataset.9b1a8f33-9824-427f-8daf-2f691f8ebe4f` lookup
    ON d.device_exposure_id_51_bit = lookup.device_exposure_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as device_exposure_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(device_exposure_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
    , v.visit_occurrence_id
    , prov.provider_id
FROM global_id
LEFT JOIN `ri.foundry.main.dataset.3b888e3f-a7f7-4251-a585-b0d0d987e29d` p
ON global_id.site_person_id = p.site_person_id
LEFT JOIN `ri.foundry.main.dataset.e5f71652-b192-48f9-aff4-b68c8ebd576a` v
ON global_id.site_visit_occurrence_id = v.site_visit_occurrence_id
LEFT JOIN `ri.foundry.main.dataset.841cfefa-0485-48b7-a360-f359ab610eac` prov
ON global_id.site_provider_id = prov.site_provider_id