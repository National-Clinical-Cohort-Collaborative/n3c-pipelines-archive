CREATE TABLE `ri.foundry.main.dataset.f1cd12a9-7491-4378-83de-97c582c67c7f` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.419a0f18-8aa3-4459-9df8-c5b8b43f09b4` m
    LEFT JOIN `ri.foundry.main.dataset.bdeb5c7e-94b8-440a-b758-eb25469128a3` lookup
    ON m.visit_occurrence_id_51_bit = lookup.visit_occurrence_id_51_bit
    AND m.hashed_id = lookup.hashed_id
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
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM global_id
INNER JOIN `ri.foundry.main.dataset.9051650f-db46-498d-b272-b654acea0e3f` p
    ON global_id.site_patid = p.site_patid
