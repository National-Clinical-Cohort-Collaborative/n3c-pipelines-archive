CREATE TABLE `ri.foundry.main.dataset.ce19c689-f73a-40f3-8749-e8e046c3e23d` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.9c9a239a-70f5-4ef9-9869-282f38c7c74b` m
    LEFT JOIN `ri.foundry.main.dataset.194ec2e6-296b-4f1e-8efd-918b6c0051d6` lookup
    ON m.condition_era_id_51_bit = lookup.condition_era_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as condition_era_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(condition_era_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT
      global_id.*
-- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM global_id
-- Inner join to remove patients who've been dropped in step04 due to not having an encounter
INNER JOIN `ri.foundry.main.dataset.ce0290bc-02a6-4fa7-883b-66c744f5a593` p
ON global_id.site_patient_num = p.site_patient_num
