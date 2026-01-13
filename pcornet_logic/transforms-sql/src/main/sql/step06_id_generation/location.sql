CREATE TABLE `ri.foundry.main.dataset.09144310-cb71-47d5-a9e3-5442423b11df` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as collision_index
    FROM `ri.foundry.main.dataset.e73b989f-e8ef-48db-b517-6a7498ca1fd0` m
    LEFT JOIN `ri.foundry.main.dataset.57c3adc0-be8f-4267-9cd3-9946c88b6c2b` lookup
    ON m.location_id_51_bit = lookup.location_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as location_id 
    FROM (
        SELECT
            *
            -- Take collision index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(location_id_51_bit, 2) + collision_index as local_id
        FROM join_conflict_id
    )
)

SELECT * FROM global_id
