CREATE TABLE `ri.foundry.main.dataset.8e764662-c433-4010-8b48-907bffa682c0`
TBLPROPERTIES (foundry_transform_profile = 'EXECUTOR_MEMORY_LARGE') AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.8e4ca1b5-475d-4272-833a-1ae1834f45e2` d
    LEFT JOIN `ri.foundry.main.dataset.11f53f28-d095-47eb-ba60-818faae20e76` lookup
    ON d.measurement_id_51_bit = lookup.measurement_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as measurement_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(measurement_id_51_bit, 2) + collision_index as local_id
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