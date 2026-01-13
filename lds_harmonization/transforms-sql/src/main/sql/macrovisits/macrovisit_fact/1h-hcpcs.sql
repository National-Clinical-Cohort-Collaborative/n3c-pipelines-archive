
--first regex to identify hcpcs -- for 'cleaner', baseline hcpcs
--list of 'not good' hcpcs sites
CREATE TABLE `ri.foundry.main.dataset.9c0dd571-0a4e-4354-ad12-81bb16b11e4d` AS
SELECT *,
      regexp_extract(replace(replace(procedure_source_value,'CPT:'),'CPT4:',''), '(^[A-Z][0-9]{1,5}$|^[0-9]{5}$)') as hcpc
FROM `ri.foundry.main.dataset.7394de3a-f3fe-4b73-9e61-12858dcd9868`
where data_partner_id not in (569, 1015, 266, 124, 664, 213, 605)


--'(^[A-Z][0-9]{1,5}$|^[0-9]{5}$)'
