
    --for sites that put hcpcs at beginning of strings
CREATE TABLE `ri.foundry.main.dataset.dde36a9a-1a56-4a83-ac77-bccf4f5e1c71` AS
select * from (

SELECT *, regexp_extract(procedure_source_value, '(^[A-Z]{1}[0-9]+|^[0-9]+)') as hcpc
FROM `ri.foundry.main.dataset.7394de3a-f3fe-4b73-9e61-12858dcd9868`
where data_partner_id in (569, 266, 664)

) a 
where length(hcpc)=5