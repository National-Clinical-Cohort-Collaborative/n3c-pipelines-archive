CREATE TABLE `ri.foundry.main.dataset.1de7526c-18e0-4182-8a5d-97b3d6c65798` AS
WITH

-- 1. Filter sites to those with 'Cancer' in their properties
cancer_sites AS (
  SELECT data_partner_id
  FROM `ri.foundry.main.dataset.d6674f53-0751-42ae-923d-c2608d76f29a`
  WHERE array_contains(enclaves, 'Cancer')
),

-- 2. Define ICD10CM conditions
icd10cm_conditions AS (
    SELECT DISTINCT con2.*
    FROM `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` con
    JOIN `ri.foundry.main.dataset.0469a283-692e-4654-bb2e-26922aff9d71` cr 
        ON con.concept_id = cr.concept_id_1 AND cr.relationship_id = 'Maps to'
    JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` con2 
        ON cr.concept_id_2 = con2.concept_id
    WHERE con.vocabulary_id = 'ICD10CM'
      AND con.domain_id = 'Condition'
      AND (
          con.concept_code LIKE 'C%' OR
          con.concept_code LIKE 'D0%' OR
          con.concept_code LIKE 'D1%' OR
          con.concept_code LIKE 'D2%' OR
          con.concept_code LIKE 'D3%' OR
          con.concept_code LIKE 'D4%'
      )
),

-- 3. Define ICDO3 standard conditions
icdo3_standard AS (
    SELECT DISTINCT *
    FROM `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772`
    WHERE vocabulary_id = 'ICDO3'
      AND standard_concept = 'S'
),

-- 4. Union these concepts to derive a list of cancer condition concepts
cancer_codes AS (
    SELECT * FROM icd10cm_conditions
    UNION
    SELECT * FROM icdo3_standard
),

-- 5. Select condition occurrence rows that pertain to these cancer codes, filtered to cancer sites
condition_person AS (
    SELECT DISTINCT p.person_id, p.global_person_id
    FROM `ri.foundry.main.dataset.17e798ef-2459-4d3a-98ef-ac515b83871a` co
    JOIN `ri.foundry.main.dataset.cd5ed556-a130-4d71-a043-b9c8f36a52c0` p 
        ON co.person_id = p.person_id
    JOIN cancer_codes cx 
        ON co.condition_concept_id = cx.concept_id
    JOIN cancer_sites s
        ON p.data_partner_id = s.data_partner_id
),

-- 6. Select NAACCR persons and add on metadata from person table, filtered to cancer sites
naaccr_person AS (
    SELECT 
        p.person_id, p.global_person_id
    FROM `ri.foundry.main.dataset.fcc4f44f-ecb4-4005-b9ef-22009bfdbbe6` n
    JOIN `ri.foundry.main.dataset.cd5ed556-a130-4d71-a043-b9c8f36a52c0` p 
        ON n.person_id = p.person_id
    JOIN cancer_sites s
        ON p.data_partner_id = s.data_partner_id
)

-- 7. Combine two cancer patient sources together to derive the phenotyped persons
SELECT person_id, global_person_id
FROM condition_person
UNION
SELECT person_id, global_person_id
FROM naaccr_person;