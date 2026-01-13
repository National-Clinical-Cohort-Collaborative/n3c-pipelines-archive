CREATE TABLE `ri.foundry.main.dataset.c7aba3de-14c4-41f5-aea4-e74d391cc34c` AS
 with condition as (
        SELECT 'Prepared' as stage, 'Condition' as domain,data_partner_id,source_standard_concept,count(*) as cnt
        FROM `ri.foundry.main.dataset.2bc69817-3043-4c0e-98ea-4a1b34e17ebf` 
        group by data_partner_id, source_standard_concept
    ),
    measurement as (
        SELECT 'Prepared' as stage, 'Measurement' as domain,data_partner_id,source_standard_concept,count(*) as cnt
        FROM `ri.foundry.main.dataset.faa10be1-8133-4767-a82e-1f9c7386fffc`
        group by data_partner_id, source_standard_concept
    ),
    device as (
        SELECT 'Prepared' as stage, 'Device' as domain,data_partner_id,source_standard_concept,count(*) as cnt
        FROM `ri.foundry.main.dataset.4ac2dfd9-bdde-4b51-99cd-4f00f855598b` 
        group by data_partner_id, source_standard_concept
    ),
    drug as (
        SELECT 'Prepared' as stage, 'Drug' as domain,data_partner_id,source_standard_concept,count(*) as cnt
        FROM `ri.foundry.main.dataset.c774a269-ddcc-4c70-b90e-4381e01450fd`
        group by data_partner_id, source_standard_concept
    ),
    procedure as (
        SELECT 'Prepared' as stage, 'Procedure' as domain,data_partner_id,source_standard_concept,count(*) as cnt
        FROM `ri.foundry.main.dataset.b6487574-b007-446a-91d9-c5b52f9cd62a`
        group by data_partner_id, source_standard_concept
    ),
    observation as (
        SELECT 'Prepared' as stage, 'Observation' as domain,data_partner_id,source_standard_concept,count(*) as cnt
        FROM `ri.foundry.main.dataset.a36c033b-8841-45a2-9a91-81105d91d4a7`
        group by data_partner_id, source_standard_concept

    ),
    lds as (
    select 'Condition' as domain, data_partner_id,co.standard_concept as source_standard_concept from `ri.foundry.main.dataset.641c645d-eacf-4a5a-983d-3296e1641f0c` 
        left join `/N3C Export Area/OMOP Vocabularies/concept` co on condition_concept_id=co.concept_id    
        where data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP'
    )
     union all 
     select 'Measurement' as domain, data_partner_id,co.standard_concept as source_standard_concept from `ri.foundry.main.dataset.1937488e-71c5-4808-bee9-0f71fd1284ac` l 
        left join `/N3C Export Area/OMOP Vocabularies/concept` co on measurement_concept_id=co.concept_id 
        where data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP'
    )
     union all
     SELECT 'Procedure' as domain, data_partner_id, co.standard_concept as source_standard_concept from `ri.foundry.main.dataset.2a3c8355-7fbf-478f-bbbf-eabb6733b04d` l 
        left join `/N3C Export Area/OMOP Vocabularies/concept` co on procedure_concept_id=co.concept_id 
        where data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP'
    )
     union ALL
     select 'Device' as domain, data_partner_id,co.standard_concept as source_standard_concept from `ri.foundry.main.dataset.dc046eb3-787a-41fd-b096-15f65eef3c5b` l 
        left join `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` co on device_concept_id=co.concept_id 
        where data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP'
    )
     union all
     select 'Drug' as domain, data_partner_id, co.standard_concept as source_standard_concept from `ri.foundry.main.dataset.3feda4dc-5389-4521-ab9e-0361ea5773fd` l 
         left join `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` co on drug_concept_id=co.concept_id 
         where data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP'
    )
     union all
     select 'Observation' as domain, data_partner_id, co.standard_concept as source_standard_concept from `ri.foundry.main.dataset.f925d142-3b25-498e-8538-4b9685c5270f` l 
        left join `ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772` co on observation_concept_id=co.concept_id 
        where data_partner_id in ( 
      SELECT data_partner_id from `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683`
      WHERE cdm_name = 'OMOP'
    )
    ),
    lds_final as (
        select 'LDS' as stage, domain,data_partner_id, source_standard_concept, count(*) as cnt from lds 
        
        group by data_partner_id, source_standard_concept, domain
    )
    select * from condition
    union ALL
    select * from measurement
    union all
    select * from procedure
    union ALL
    select * from observation
    union ALL
    select * from device
    union ALL
    select * from drug
    union ALL
    select * from lds_final