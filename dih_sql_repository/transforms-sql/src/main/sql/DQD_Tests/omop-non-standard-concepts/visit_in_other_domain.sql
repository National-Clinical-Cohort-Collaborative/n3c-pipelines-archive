CREATE TABLE `ri.foundry.main.dataset.963d1b28-a4e5-4a18-91dd-220dabeafdf6` AS

with condition as (   
    SELECT data_partner_id , target_domain_id, count(*) as cnt FROM `ri.foundry.main.dataset.2bc69817-3043-4c0e-98ea-4a1b34e17ebf` 
    group by data_partner_id,target_domain_id
    having target_domain_id ='Visit'
),
observation as (
    SELECT data_partner_id , target_domain_id, count(*) as cnt FROM `ri.foundry.main.dataset.a36c033b-8841-45a2-9a91-81105d91d4a7` 
    group by data_partner_id,target_domain_id
    having target_domain_id ='Visit'
),
procedure as (
    SELECT data_partner_id , target_domain_id, count(*) as cnt FROM `ri.foundry.main.dataset.b6487574-b007-446a-91d9-c5b52f9cd62a` 
    group by data_partner_id,target_domain_id
    having target_domain_id ='Visit'
),
measurement as (
    SELECT data_partner_id , target_domain_id, count(*) as cnt FROM `ri.foundry.main.dataset.faa10be1-8133-4767-a82e-1f9c7386fffc` 
    group by data_partner_id,target_domain_id
    having target_domain_id ='Visit'
),
drug as (
    SELECT data_partner_id , target_domain_id, count(*) as cnt FROM `ri.foundry.main.dataset.c774a269-ddcc-4c70-b90e-4381e01450fd` 
    group by data_partner_id,target_domain_id
    having target_domain_id ='Visit'
),
device as (
    SELECT data_partner_id , target_domain_id, count(*) as cnt FROM `ri.foundry.main.dataset.4ac2dfd9-bdde-4b51-99cd-4f00f855598b` 
    group by data_partner_id,target_domain_id
    having target_domain_id ='Visit'
)

select 'Condtion' as source_domain, * from condition
union ALL
select 'Measurement' as source_domain, * from measurement
union ALL
select 'Drug' as source_domain, * from drug
union ALL
select 'Device' as source_domain, * from device
union all
select 'Procedure' as source_domain, * from procedure
union ALL
select 'Observation' as source_domain, * from observation