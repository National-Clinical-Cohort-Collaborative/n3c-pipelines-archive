CREATE TABLE `ri.foundry.main.marketplace-repository-integration.e7e62014-213a-4c37-8c79-ac42cc31ad47_period` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.marketplace-repository-integration.0525347e-6a84-495a-8137-402db6dab0bc_period` m
    LEFT JOIN `ri.foundry.main.dataset.ecd6ecf3-5fb3-4fb4-9667-0211298484d3` lookup
    ON m.observation_period_id_51_bit = lookup.observation_period_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as observation_period_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(observation_period_id_51_bit, 2) + collision_index as local_id
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
