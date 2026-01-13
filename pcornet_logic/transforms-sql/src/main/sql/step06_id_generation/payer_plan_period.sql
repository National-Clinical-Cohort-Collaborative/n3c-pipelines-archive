CREATE TABLE `ri.foundry.main.dataset.24f8c03a-af20-4c9f-b860-ef4e075dbb43` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.01dd1a33-c65f-4f13-8d20-03d9093a94a2` m
    LEFT JOIN `ri.foundry.main.dataset.d5808483-5489-4286-bd1f-ab427a442e6b` lookup
    ON m.payer_plan_period_id_51_bit = lookup.payer_plan_period_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as payer_plan_period_id 
    FROM (
        SELECT
            *
            -- Take conflict index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(payer_plan_period_id_51_bit, 2) + collision_index as local_id
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