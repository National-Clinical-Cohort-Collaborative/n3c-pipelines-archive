CREATE TABLE `ri.foundry.main.dataset.08b50842-6274-4982-b44a-8e2fa23dd83e` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.f2557c45-d0f5-42ae-a8d7-43301380be00` m
    LEFT JOIN `ri.foundry.main.dataset.1b28f1a4-c9bd-453a-b355-e6fa0163b62e` lookup
    ON m.device_exposure_id_51_bit = lookup.device_exposure_id_51_bit
    AND m.hashed_id = lookup.hashed_id
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
FROM global_id
INNER JOIN `ri.foundry.main.dataset.9051650f-db46-498d-b272-b654acea0e3f` p
    ON global_id.site_patid = p.site_patid
LEFT JOIN `ri.foundry.main.dataset.f1cd12a9-7491-4378-83de-97c582c67c7f` v
    ON global_id.site_encounterid = v.site_encounterid
    and v.domain_source != 'PROCEDURES'