CREATE TABLE `ri.foundry.main.dataset.a809efea-f818-4a69-a9cc-38cb27f778bc` AS

SELECT
      d.*
    -- Join in the final person and visit ids from the final OMOP domains after collision resolutions
    , p.person_id
FROM `ri.foundry.main.dataset.97386241-9e4d-43cb-a7f8-c34b2c636234` d
-- Inner join to remove patients who've been dropped in step04 due to not having an encounter
INNER JOIN `ri.foundry.main.dataset.ce0290bc-02a6-4fa7-883b-66c744f5a593` p
  ON d.site_patient_num = p.site_patient_num
