CREATE TABLE `ri.foundry.main.dataset.841cfefa-0485-48b7-a360-f359ab610eac` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.5025baee-cb2f-4d99-a262-0d159dea593f` d
    LEFT JOIN `ri.foundry.main.dataset.62a1130d-add2-4677-a8ba-a3503e61133d` lookup
    ON d.provider_id_51_bit = lookup.provider_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as provider_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(provider_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , care.care_site_id
FROM global_id
LEFT JOIN `ri.foundry.main.dataset.267c41ad-bc83-438b-8f99-ad573fc7a7f9` care
ON global_id.site_care_site_id = care.site_care_site_id