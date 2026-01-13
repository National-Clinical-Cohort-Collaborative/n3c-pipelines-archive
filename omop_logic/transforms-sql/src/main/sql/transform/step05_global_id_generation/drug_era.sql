CREATE TABLE `ri.foundry.main.dataset.d87524cd-414e-4179-b377-3984c27dfb19` AS

WITH join_conflict_id AS (
    SELECT 
        d.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.21a72f8d-4c45-47aa-9b93-652bd6feab5c` d
    LEFT JOIN `ri.foundry.main.dataset.c5c95243-be1e-41ed-93d3-1ae2d9cc2182` lookup
    ON d.drug_era_id_51_bit = lookup.drug_era_id_51_bit
    AND d.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as drug_era_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(drug_era_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM global_id
LEFT JOIN `ri.foundry.main.dataset.3b888e3f-a7f7-4251-a585-b0d0d987e29d` p
ON global_id.site_person_id = p.site_person_id
