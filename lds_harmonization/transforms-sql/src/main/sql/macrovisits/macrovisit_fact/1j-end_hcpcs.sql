
--for sites that put hcpcs at end of strings
CREATE TABLE `ri.foundry.main.dataset.80477e83-0dd7-4949-b9f1-1be44e95fda3` AS
select * from (

SELECT *, regexp_extract(procedure_source_value, '([A-Z]{1}[0-9]+$|[0-9]+$)') as hcpc
FROM `ri.foundry.main.dataset.7394de3a-f3fe-4b73-9e61-12858dcd9868`
where data_partner_id in (1015, 124, 213, 605)

) A
where length(hcpc)=5