
--take out apparent blank drg observations & CPT 99072 that includes 'DRG'
--also deduplicate observations where multiple DRGs (eg, APR & MS) are submitted for same visit
CREATE TABLE `ri.foundry.main.dataset.5b45767e-fa7e-43db-bc5f-001195db02ce` AS
select * from (

SELECT distinct *, row_number() over (partition by person_id order by observation_source_value) as row
FROM `ri.foundry.main.dataset.9460eec1-a5ee-457d-ad56-e7a22fd4536a` 
where observation_source_value !='DRGID:   DRG:'
and observation_source_value !='99072|PR ADDL SUPL MATRL&STAF TM DRG PHE RES-TR NF'
and observation_source_value is not null

) a 
where row=1