
CREATE TABLE `ri.foundry.main.dataset.59aff609-424e-43de-a783-b21723ef2084` AS

select 
re.domain as rescued_domain,
re.data_partner_id as rescued_data_partner_id,
re.type as rescued_type,

re.num_records as rescued_num_records, 
re.new_tot_num_records as rescued_tot_num_records,
--re.percentage as rescued_percentage,

c.domain as domain,
c.data_partner_id as data_partner_id,
c.type as type,

c.num_records as num_records,
c.tot_num_records as tot_num_records,
c.percentage as percentage,
re.percentage as rescued_percentage

FROM `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/Rescued_concept_checks_aggregated` re
full outer join `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/omop-non-standard-concepts/concept_checks_aggregated` c
on re.domain=c.domain and re.data_partner_id=c.data_partner_id and re.type=c.type
