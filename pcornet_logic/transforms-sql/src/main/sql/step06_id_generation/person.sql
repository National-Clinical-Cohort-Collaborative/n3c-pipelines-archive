CREATE TABLE `ri.foundry.main.dataset.9051650f-db46-498d-b272-b654acea0e3f` AS

WITH join_conflict_id AS (
    SELECT 
        m.*
        , COALESCE(lookup.collision_bits, 0) as id_index
    FROM `ri.foundry.main.dataset.139dc174-3e09-4ed7-bec0-28d70d5b550a` m
    LEFT JOIN `ri.foundry.main.dataset.38a0c589-55cf-47db-8a27-4dc9c95f067e` lookup
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
), 

cte_addr as (
    -- Get most recent address for each patient
    SELECT * FROM (
        SELECT 
            addressid,
            patid,
            address_city,
            address_state,
            address_zip5,
            address_period_start,
            address_period_end,
            data_partner_id,
            payload,
            Row_Number() Over (Partition By patid Order By COALESCE(address_period_end, CURRENT_DATE()) Desc, address_period_start Desc) as addr_rank
        FROM `ri.foundry.main.dataset.5d998a43-1e3d-40d1-85fa-e44564cef17f` addr_hist
    )
    WHERE addr_rank = 1
),

pat_to_loc_id_map AS (
    SELECT 
        cte_addr.patid as site_patid,
        loc.*
    FROM cte_addr
        LEFT JOIN `ri.foundry.main.dataset.09144310-cb71-47d5-a9e3-5442423b11df` loc
        ON address_city = loc.city
        AND address_state = loc.state
        AND address_zip5 = loc.zip
)

SELECT
      global_id.*
    -- Join in the final location id after collision resolutions
    , pat_to_loc_id_map.location_id
FROM global_id
LEFT JOIN pat_to_loc_id_map
    ON global_id.site_patid = pat_to_loc_id_map.site_patid
