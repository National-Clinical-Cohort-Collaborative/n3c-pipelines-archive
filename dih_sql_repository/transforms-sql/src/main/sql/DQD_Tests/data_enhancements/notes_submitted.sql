CREATE TABLE `ri.foundry.main.dataset.08d68abe-52e9-40ec-8a2b-85dff65d8325` AS
    SELECT nt.data_partner_id, 
    mn.cdm_name,
    count(distinct nt.note_id) as note_count
    FROM `ri.foundry.main.dataset.39da274e-0c28-4712-adca-3ad7ca7d976a` nt
    left join `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mn
    on nt.data_partner_id = mn.data_partner_id
    group by
    nt.data_partner_id,
    mn.cdm_name