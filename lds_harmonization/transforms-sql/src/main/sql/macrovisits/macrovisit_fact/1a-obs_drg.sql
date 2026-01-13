CREATE TABLE `ri.foundry.main.dataset.9460eec1-a5ee-457d-ad56-e7a22fd4536a` AS
SELECT *, regexp_extract(observation_source_value, '([0-9]{3}$)') as drg
FROM `ri.foundry.main.dataset.1cb3fc9f-0fc6-4dbd-a1ad-da8bcac8da2a`
where upper(observation_source_value) like ('%DRG%')