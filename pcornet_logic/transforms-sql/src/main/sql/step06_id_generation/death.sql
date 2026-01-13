CREATE TABLE `ri.foundry.main.dataset.21ec0519-a902-418c-b752-8829bba1ef0d` AS

SELECT
      d.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM `ri.foundry.main.dataset.071a4b37-c925-4738-bc46-d8af8676d885` d
-- Inner join to remove patients who've been dropped in step04 due to not having a visit/record
INNER JOIN `ri.foundry.main.dataset.9051650f-db46-498d-b272-b654acea0e3f` p
  ON d.site_patid = p.site_patid
