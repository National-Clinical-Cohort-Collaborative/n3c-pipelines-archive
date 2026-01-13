CREATE TABLE `ri.foundry.main.dataset.1d757361-0da6-43b4-8a37-e6ef5a1168f3` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.9b7f68eb-3564-4cb3-b019-659f7e49c5af` m
    LEFT JOIN `ri.foundry.main.dataset.98a14d44-49fc-4e29-b064-1147b4e64ccd` lookup
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
INNER JOIN `ri.foundry.main.dataset.9051650f-db46-498d-b272-b654acea0e3f` p
    ON global_id.site_patid = p.site_patid
