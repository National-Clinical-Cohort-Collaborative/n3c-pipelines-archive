CREATE TABLE `ri.foundry.main.dataset.c04b8f5d-7069-4f06-9cb8-7d9bbb4a287c` AS
WITH

-- 1. Filter sites where 'Renal' is in the enclaves array
renal_sites AS (
  SELECT data_partner_id
  FROM `ri.foundry.main.dataset.d6674f53-0751-42ae-923d-c2608d76f29a`
  WHERE array_contains(enclaves, 'Renal')
),

-- 2. Identify ICD10CM conditions of interest (your logic unchanged)
icd10cm_conditions AS (
  SELECT DISTINCT con2.*
  FROM `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` con
  JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr ON con.concept_id = cr.concept_id_1 AND cr.relationship_id = 'Maps to'
  JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` con2 ON cr.concept_id_2 = con2.concept_id
  WHERE con.vocabulary_id = 'ICD10CM'
    AND con.domain_id = 'Condition'
    AND (
      con.concept_code LIKE 'N17%' OR
      con.concept_code LIKE 'N18.1%' OR
      con.concept_code LIKE 'N18.2%' OR
      con.concept_code LIKE 'N18.30%' OR
      con.concept_code LIKE 'N18.31%' OR
      con.concept_code LIKE 'N18.32%' OR
      con.concept_code LIKE 'N18.4%' OR
      con.concept_code LIKE 'N18.5%' OR
      con.concept_code LIKE 'N18.6%' OR
      con.concept_code LIKE 'N18.9%' OR
      con.concept_code LIKE 'E08.22%' OR
      con.concept_code LIKE 'E09.22%' OR
      con.concept_code LIKE 'E10.22%' OR
      con.concept_code LIKE 'E11.22%' OR
      con.concept_code LIKE 'E13.22%' OR
      con.concept_code LIKE 'I12.0%' OR
      con.concept_code LIKE 'I12.9%' OR
      con.concept_code LIKE 'I13.0%' OR
      con.concept_code LIKE 'I13.10%' OR
      con.concept_code LIKE 'I13.11%'
    )
), 

-- 3. Persons from renal sites only
renal_persons AS (
  SELECT person_id, global_person_id, data_partner_id
  FROM `ri.foundry.main.dataset.cd5ed556-a130-4d71-a043-b9c8f36a52c0`
  WHERE data_partner_id IN (SELECT data_partner_id FROM renal_sites)
),

-- 4. Filtered condition occurrence for persons from renal sites only
condition_person AS (
  SELECT DISTINCT p.person_id, p.global_person_id
  FROM `ri.foundry.main.dataset.17e798ef-2459-4d3a-98ef-ac515b83871a` co
  JOIN renal_persons p ON co.person_id = p.person_id
  JOIN icd10cm_conditions cx ON co.condition_concept_id = cx.concept_id
)

-- 5. Final output
SELECT * FROM condition_person