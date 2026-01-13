CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/data_enhancements/note_types` AS
    SELECT 
    nt.data_partner_id,
    mn.cdm_name,
    'Note Type' as concept,
    nt.note_type_concept_name as concept_name,
    count(*) as row_count
    FROM `ri.foundry.main.dataset.39da274e-0c28-4712-adca-3ad7ca7d976a` nt
    left join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mn
    on nt.data_partner_id = mn.data_partner_id
    group by 
    nt.data_partner_id,
    mn.cdm_name,
    nt.note_type_concept_name
    UNION
        SELECT 
    nt.data_partner_id,
    mn.cdm_name,
    'Note Class' as concept,
    nt.note_class_concept_name as concept_name,
    count(*) as row_count
    --
    FROM `ri.foundry.main.dataset.39da274e-0c28-4712-adca-3ad7ca7d976a` nt
    left join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mn
    on nt.data_partner_id = mn.data_partner_id
    group by 
    nt.data_partner_id,
    mn.cdm_name,
    nt.note_class_concept_name