CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/visits/Site_LOS_IP` AS

 with calc_LOS as (  
    SELECT data_partner_id, datediff(visit_end_date, visit_start_date) as LOS, visit_start_date, visit_end_date, visit_occurrence_id
    FROM `ri.foundry.main.dataset.749e3f71-9b0e-4896-af8b-91aefe71b308`
    where visit_concept_id IN (9201, 32037,581379, 581383, 262, 8717)
 ) 
 select data_partner_id, LOS, count(distinct visit_occurrence_id) as Visit_count
 from calc_LOS 
 group by data_partner_id, LOS
 order by data_partner_id, LOS