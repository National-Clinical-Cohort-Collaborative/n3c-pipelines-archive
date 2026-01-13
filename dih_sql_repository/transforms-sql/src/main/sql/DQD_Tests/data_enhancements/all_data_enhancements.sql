CREATE TABLE `ri.foundry.main.dataset.143bc9da-d870-4b8b-8c8b-e45adef4682a` AS
WITH O2 as 
    (SELECT  
    o2.data_partner_id, 
    sum(o2.num_rows) as row_count
FROM `ri.foundry.main.dataset.e134b541-36c3-487c-8454-a22a97c05be0` o2
group by o2.data_partner_id
    ),
sdoh as 
    (SELECT  
    sd.data_partner_id, 
    sum(sd.row_count) as row_count
FROM `ri.foundry.main.dataset.676c56e7-474d-4b2a-9da6-50194eb1a0b9` sd
group by sd.data_partner_id
    )
select     
    cast(mc.data_partner_id as INT),  
    mc.cdm_name, 
    'O2 Device' as  data_enhancement,
    coalesce(o2.row_count, 0) as row_count
    FROM `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mc
left join O2 o2
on mc.data_partner_id = o2.data_partner_id
union
select    	
    cast(mc.data_partner_id as INT),  	
    mc.cdm_name,  	
    'ADT Transactions' as data_enhancement,	
    coalesce(adt.total_visits, 0) as row_count   	
    FROM `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mc	
left join `ri.foundry.main.dataset.058a412e-7d7f-4829-8097-afe06bd94808` adt
on mc.data_partner_id = adt.data_partner_id
UNION	
SELECT	
    cast(mc.data_partner_id as INT),  	
    mc.cdm_name,  	
    'Long COVID Clinic Visits' as data_enhancement,	
    coalesce(lcc.row_count, 0) as row_count   	
    FROM `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mc	
left join `ri.foundry.main.dataset.0270e7fe-4d0e-424e-866c-51cbda51f0ac` lcc
on mc.data_partner_id = lcc.data_partner_id
UNION
SELECT
    cast(mc.data_partner_id as INT),  
    mc.cdm_name, 
    'SDOH' as data_enhancement,	
    coalesce(sdoh.row_count, 0) as row_count	
    FROM `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mc	
left join sdoh
on mc.data_partner_id = sdoh.data_partner_id
UNION	
SELECT	
    cast(mc.data_partner_id as INT),  	
    mc.cdm_name, 	
    'Notes' as data_enhancement,	
    coalesce(nt.note_count, 0) as row_count	
    FROM `ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683` mc	
left join `ri.foundry.main.dataset.08d68abe-52e9-40ec-8a2b-85dff65d8325` nt	
on mc.data_partner_id = nt.data_partner_id