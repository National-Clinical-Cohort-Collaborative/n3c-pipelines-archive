CREATE TABLE `ri.foundry.main.dataset.ce0290bc-02a6-4fa7-883b-66c744f5a593` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as id_index
    FROM `ri.foundry.main.dataset.8ee9d6c1-a876-4756-93d5-af3d644eac31` m
    LEFT JOIN `ri.foundry.main.dataset.efecaced-8ace-4296-aab0-76f7b3965053` lookup
    ON m.person_id_51_bit = lookup.person_id_51_bit
    AND m.hashed_id = lookup.hashed_id
),

global_id AS (
SELECT
      *
    -- Final 10 bits reserved for the site id
    , shiftleft(local_id, 10) + data_partner_id as person_id 
    FROM (
        SELECT
            *
            -- Take collision index and append it as 2 bits (assumes no more than 3 conflicts)
            , shiftleft(person_id_51_bit, 2) + id_index as local_id
        FROM join_conflict_id
    )
) 

SELECT
      global_id.*
-- Join in the final location id after collision resolutions
    , loc.location_id
FROM global_id
LEFT JOIN `ri.foundry.main.dataset.46d64423-9961-4e85-bc80-39ea30a4dd85` loc
    ON global_id.location_hashed_id = loc.hashed_id
