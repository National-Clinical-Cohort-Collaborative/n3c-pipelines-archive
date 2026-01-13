CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/death/death` AS
--merge person table with death table

    SELECT   d.data_partner_id, d.death_date, d.person_id, date_part('YEAR', d.death_date) as year_of_death , p.year_of_birth           
                
        FROM `ri.foundry.main.dataset.dfde51a2-d775-440a-b436-f1561d3f8e5d` d
        JOIN `ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239` p 
        ON d.person_id = p.person_id
        where d.death_date is not null
        

