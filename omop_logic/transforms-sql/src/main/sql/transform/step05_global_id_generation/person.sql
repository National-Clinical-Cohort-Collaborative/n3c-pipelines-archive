CREATE TABLE `ri.foundry.main.dataset.3b888e3f-a7f7-4251-a585-b0d0d987e29d` AS


WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as id_index
    FROM `ri.foundry.main.dataset.d35dbdfd-08f4-42de-a65f-38ed377476dd` d
    LEFT JOIN `ri.foundry.main.dataset.1be8a50a-dd0c-4460-8f9d-bff773bfca9d` lookup
    ON d.person_id_51_bit = lookup.person_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as person_id 
    FROM (
        SELECT
            *
            -- Take collision index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(person_id_51_bit, 2) + id_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , loc.location_id
    , prov.provider_id
    , care.care_site_id
FROM global_id
LEFT JOIN `ri.foundry.main.dataset.cf7fb682-fd88-4cc9-b6a4-d86a275692c4` loc
ON global_id.site_location_id = loc.site_location_id
LEFT JOIN `ri.foundry.main.dataset.841cfefa-0485-48b7-a360-f359ab610eac` prov
ON global_id.site_provider_id = prov.site_provider_id
LEFT JOIN `ri.foundry.main.dataset.267c41ad-bc83-438b-8f99-ad573fc7a7f9` care
ON global_id.site_care_site_id = care.site_care_site_id