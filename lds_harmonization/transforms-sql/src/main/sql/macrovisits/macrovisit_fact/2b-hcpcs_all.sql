
--combine extracted hcpc data back together minus places where hcpc is null or empty

CREATE TABLE `ri.foundry.main.dataset.f4a5f0c9-4902-42fd-b362-dbc1d79e97f8` TBLPROPERTIES (foundry_transform_profiles = 'EXECUTOR_MEMORY_LARGE, NUM_EXECUTORS_16,EXECUTOR_MEMORY_OVERHEAD_EXTRA_LARGE') AS

SELECT * FROM `ri.foundry.main.dataset.9c0dd571-0a4e-4354-ad12-81bb16b11e4d`
where length(hcpc)>0

UNION

select * from `ri.foundry.main.dataset.dde36a9a-1a56-4a83-ac77-bccf4f5e1c71`
where length(hcpc)>0

UNION

select * from `ri.foundry.main.dataset.80477e83-0dd7-4949-b9f1-1be44e95fda3`
where length(hcpc)>0