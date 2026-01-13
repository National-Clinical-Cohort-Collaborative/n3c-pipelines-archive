CREATE TABLE `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/04 - mapping/dqp_procedure_in_visit` AS
with 
procedure as (
        SELECT p.data_partner_id, count(*) as procedure_with_visit_cnt
        FROM `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/03 - prepared/procedure_occurrence` p
        WHERE procedure_occurrence_id IS NOT NULL
        and p.target_domain_id = 'Visit' and p.target_standard_concept = 'S'
        group by data_partner_id
),visit as (
select v.data_partner_id, count(*) as visit_incoming_cnt 
from `/UNITE/Data Ingestion & OMOP Mapping/Source Data Model: PEDSnet/Site 1015/transform/04 - mapping/visit_occurrence` v
where source_domain = 'PROCEDURE'
group by data_partner_id
)
select p.*,v.visit_incoming_cnt  from procedure p
left join visit v
on p.data_partner_id = v.data_partner_id