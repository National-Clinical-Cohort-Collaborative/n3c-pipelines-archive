CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/sdoh/sdoh_questions` AS
with most_recent_sdoh_questions_concept_id as (
    select distinct 
        concept_id 
    from 
        `/N3C Export Area/Concept Set Ontology/Concept Set Ontology/hubble_base/concept_set_members`
    where 
        concept_set_name = 'sdoh_questions' and is_most_recent_version = 'true' 
),
sdoh_questions as (
SELECT /*+ BROADCAST(most_recent_sdoh_questions_concept_id) */ 
    observation.data_partner_id,
    observation.person_id, 
    observation.observation_concept_name as sdoh_question_concept_name
FROM `/UNITE/LDS/clean/observation` observation
inner join most_recent_sdoh_questions_concept_id
    on
        observation.observation_concept_id = most_recent_sdoh_questions_concept_id.concept_id
)
select 
    data_partner_id, 
    sdoh_question_concept_name, 
    count(distinct person_id) as people_w_sdoh_question
from sdoh_questions
GROUP BY 
    data_partner_id, 
    sdoh_question_concept_name
