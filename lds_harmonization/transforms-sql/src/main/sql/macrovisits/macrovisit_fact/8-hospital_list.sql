

CREATE TABLE `ri.foundry.main.dataset.56529a15-8d02-4ab9-82ae-ee67a1ce8730` AS
SELECT macrovisit_id, 
1 as likely_hospitalization
FROM `ri.foundry.main.dataset.755f18d4-f44e-4dfe-b8ae-21f507f1bd74`
where (drg is not null or max_cms_inpt=1 or max_resources>=50 or all_inpt=1 or all_icu=1)

