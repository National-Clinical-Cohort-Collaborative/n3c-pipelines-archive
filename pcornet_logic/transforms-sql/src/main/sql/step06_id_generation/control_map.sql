CREATE TABLE `ri.foundry.main.dataset.38ea0427-e3b9-4209-8de4-86d8eacafaf5` AS
  
WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.8539a730-19b9-4109-ab25-1c007cb4e3ae` m
    LEFT JOIN `ri.foundry.main.dataset.0ce78000-1d4c-4a11-a97c-23797fda6abf` lookup
    ON m.control_map_id_51_bit = lookup.control_map_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as control_map_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(control_map_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
-- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id as case_person_id
    , pp.person_id as control_person_id
FROM global_id
INNER JOIN `ri.foundry.main.dataset.9051650f-db46-498d-b272-b654acea0e3f` p
    ON global_id.site_case_person_id = p.site_patid
LEFT JOIN `ri.foundry.main.dataset.9051650f-db46-498d-b272-b654acea0e3f` pp
    ON global_id.site_control_person_id = pp.site_patid
