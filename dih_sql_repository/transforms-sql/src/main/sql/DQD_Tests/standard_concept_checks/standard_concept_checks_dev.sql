CREATE TABLE `ri.foundry.main.dataset.e80374b9-4dc2-4c85-923c-668080bb93e4` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_8, EXECUTOR_MEMORY_SMALL') AS
WITH
    -- selecting 2 columns instead of whole table's width seems to cut time to 2/3.
 NARROW_MEAS as (select measurement_concept_id, data_partner_id FROM `/UNITE/LDS/clean/measurement` ),
 NARROW_COND as (select condition_concept_id, data_partner_id  FROM `/UNITE/LDS/clean/condition_occurrence` ),
 NARROW_DRUG as (select drug_concept_id, data_partner_id FROM `/UNITE/LDS/clean/drug_exposure` ),
 NARROW_OBS as ( SELECT observation_concept_id, data_partner_id FROM `/UNITE/LDS/clean/observation` ),
 NARROW_PROC as (select procedure_concept_id, data_partner_id FROM `/UNITE/LDS/clean/procedure_occurrence` ),
 NARROW_VISIT as (select visit_concept_id, data_partner_id FROM `/UNITE/LDS/clean/visit_occurrence` ),

-- NB case/when as used below creates a row with value 1 when the logical condition is true. It
--   creates nothing when the logical condition is false, so the count() counts the trues. 
-- OK_LOCAL_CONCEPTS are (2004208004, 2004208005, 2004207791)

A as (
    SELECT 
        count(*) as ct, -- total rows in domain (it's a left join)
        count(case when m.measurement_concept_id is null then 1 end) as null_concept, -- nulls in domain
        count(case when m.measurement_concept_id is not null and c.concept_id = 0 then 1 end) as no_matching_concept, 
        count(case when m.measurement_concept_id is not null and c.concept_id is null 
              and m.measurement_concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as mystery_concept, 
        -- non_standard does not include no_matching_concept, though it is a non_standard concept.        
        count(case when c.concept_id is not null and (c.standard_concept is null OR c.standard_concept != 'S')  
              and c.concept_id != 0 and c.concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as non_standard, 
        count(case when c.concept_id is not null and c.standard_concept is not null and c.standard_concept = 'S' 
        then 1 end) as standard, 
        -- Attempt at duplicating the first row in DQD-StdConcepts tab (it fails, concept_id is null works)
        -- This matches the non_standard column here.
        -- count(case when (c.standard_concept is null OR c.standard_concept != 'S') 
        --                 AND (concept_id !=0 OR concept_id is not null) then 1 end ) as OMG2,
        m.data_partner_id,
        'measurement' as domain
    FROM NARROW_MEAS m
    -- LEFT JOIN `/UNITE/OMOP Vocabularies/concept` c
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON (m.measurement_concept_id = c.concept_id)
    GROUP BY data_partner_id 

    UNION ALL

    SELECT 
        count(*) as ct, -- total
        count(case when co.condition_concept_id is null then 1 end) as null_concept,
        count(case when co.condition_concept_id is not null and c.concept_id = 0 then 1 end) as no_matching_concept,
        count(case when co.condition_concept_id is not null and c.concept_id is null 
              and co.condition_concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as mystery_concept,
        count(case when c.concept_id is not null and (c.standard_concept is null OR c.standard_concept != 'S')  and c.concept_id != 0 
            and c.concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as non_standard,
        count(case when c.concept_id is not null and c.standard_concept is not null and c.standard_concept = 'S' then 1 end) as standard,
        co.data_partner_id,
        'condition_occurrence' as domain
    FROM NARROW_COND co
    -- LEFT JOIN `/UNITE/OMOP Vocabularies/concept` c
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON (co.condition_concept_id = c.concept_id)
    GROUP BY data_partner_id 

    UNION ALL

    SELECT 
        count(*) as ct, -- total
        count(case when de.drug_concept_id is null then 1 end) as null_concept,
        count(case when de.drug_concept_id is not null and c.concept_id = 0 then 1 end) as no_matching_concept,
        count(case when de.drug_concept_id is not null and c.concept_id is null 
               and de.drug_concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as mystery_concept,
        count(case when c.concept_id is not null and (c.standard_concept is null OR c.standard_concept != 'S')  and c.concept_id != 0 
               and c.concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as non_standard,
        count(case when c.concept_id is not null and c.standard_concept is not null and c.standard_concept = 'S' then 1 end) as standard,
        de.data_partner_id,
        'drug_exposure' as domain
    FROM NARROW_DRUG de
    -- LEFT JOIN `/UNITE/OMOP Vocabularies/concept` c
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON (de.drug_concept_id = c.concept_id)
    GROUP BY data_partner_id 

    UNION ALL

    SELECT 
        count(*) as ct, -- total
        count(case when o.observation_concept_id is null then 1 end) as null_concept,
        count(case when o.observation_concept_id is not null and c.concept_id = 0 then 1 end) as no_matching_concept,
        count(case when o.observation_concept_id is not null and c.concept_id is null then 1 end) as mystery_concept,
        count(case when c.concept_id is not null and (c.standard_concept is null OR c.standard_concept != 'S')  and c.concept_id != 0 
               and o.observation_concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as non_standard,
        count(case when c.concept_id is not null and c.standard_concept is not null and c.standard_concept = 'S' 
               and c.concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as standard,
        o.data_partner_id,
        'observation' as domain
    FROM NARROW_OBS o
    -- LEFT JOIN `/UNITE/OMOP Vocabularies/concept` c
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c    
    ON (o.observation_concept_id = c.concept_id)
    GROUP BY data_partner_id 

    UNION ALL
    SELECT 
        count(*) as ct, -- total
        count(case when po.procedure_concept_id is null then 1 end) as null_concept,
        count(case when po.procedure_concept_id is not null and c.concept_id = 0 
               and po.procedure_concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as no_matching_concept,
        count(case when po.procedure_concept_id is not null and c.concept_id is null 
               and c.concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as mystery_concept,
        count(case when c.concept_id is not null and (c.standard_concept is null OR c.standard_concept != 'S')  and c.concept_id != 0 then 1 end) as non_standard,
        count(case when c.concept_id is not null and c.standard_concept is not null and c.standard_concept = 'S' then 1 end) as standard,
        po.data_partner_id,
        'procedure_occurrence' as domain
    FROM NARROW_PROC po
    -- LEFT JOIN `/UNITE/OMOP Vocabularies/concept` c
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON (po.procedure_concept_id = c.concept_id)
    GROUP BY data_partner_id 

    UNION ALL

    SELECT 
        count(*) as ct, -- total
        count(case when vo.visit_concept_id is null then 1 end) as null_concept,
        count(case when vo.visit_concept_id is not null and c.concept_id = 0 
               and vo.visit_concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as no_matching_concept,
        count(case when vo.visit_concept_id is not null and c.concept_id is null 
               and c.concept_id not in (2004208004, 2004208005, 2004207791) then 1 end) as mystery_concept,
        count(case when c.concept_id is not null and (c.standard_concept is null OR c.standard_concept != 'S')  and c.concept_id != 0 then 1 end) as non_standard,
        count(case when c.concept_id is not null and c.standard_concept is not null and c.standard_concept = 'S' then 1 end) as standard,
        vo.data_partner_id,
        'visit_occurrence' as domain 
    FROM NARROW_VISIT vo
    -- LEFT JOIN `/UNITE/OMOP Vocabularies/concept` c
    LEFT JOIN `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` c
    ON (vo.visit_concept_id = c.concept_id)
     GROUP BY data_partner_id 
)
select data_partner_id, domain, 
    ct,
    null_concept, 
    no_matching_concept, 
    mystery_concept, 
    non_standard, 
    standard,
    case when null_concept + no_matching_concept + mystery_concept + non_standard + standard != ct then true else false end as error 
from A
order by data_partner_id, domain
