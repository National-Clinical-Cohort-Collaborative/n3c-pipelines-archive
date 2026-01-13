
--identify visits with likely covid diagnosis from concepts
CREATE TABLE `ri.foundry.main.dataset.959048f1-4946-4acc-9bf9-7bee79c54fc1` AS
SELECT 1 as covid_dx, *
FROM `ri.foundry.main.dataset.17e798ef-2459-4d3a-98ef-ac515b83871a`
where lower(condition_concept_name) like ('%cov%')
