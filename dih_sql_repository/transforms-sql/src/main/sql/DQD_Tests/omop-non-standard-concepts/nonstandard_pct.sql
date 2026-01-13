CREATE TABLE `ri.foundry.main.dataset.3ecdda9b-493a-437d-bb1e-b691ad1a7a55` AS
    
SELECT stage,domain,data_partner_id,sum(case when source_standard_concept is null then cnt else 0 end) /sum(cnt)*100 pre_mapping_nonstandard_cnt_pct
FROM `ri.foundry.main.dataset.c7aba3de-14c4-41f5-aea4-e74d391cc34c`
group by data_partner_id, domain,stage