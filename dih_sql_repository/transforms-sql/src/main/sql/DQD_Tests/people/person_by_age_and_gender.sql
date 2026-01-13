CREATE TABLE `ri.foundry.main.dataset.2801e588-6936-4fe2-971e-8c7df38825fa` AS
    with person as (
    select data_partner_id, person_id, gender_concept_name, year_of_birth, year(current_timestamp) - year_of_birth as january_1st_age 
    FROM `ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239`
    ),
    bracket as (
        SELECT person_id, data_partner_id, gender_concept_name, year_of_birth, january_1st_age,
        CASE WHEN january_1st_age < 10 THEN '< 10'
            WHEN january_1st_age >= 10 and january_1st_age < 20 then '10 - 19'
            WHEN january_1st_age >= 20 and january_1st_age < 30 then '20 - 29'
            WHEN january_1st_age >= 30 and january_1st_age < 40 then '30 - 39'
            WHEN january_1st_age >= 40 and january_1st_age < 50 then '40 - 49'
            WHEN january_1st_age >= 50 and january_1st_age < 60 then '50 - 59'
            WHEN january_1st_age >= 60 and january_1st_age < 70 then '60 - 69'
            WHEN january_1st_age >= 70 and january_1st_age < 80 then '70 - 79'
            WHEN january_1st_age >= 80 and january_1st_age < 90 then '80 - 89'
            WHEN january_1st_age > 89 then '> 89'
            else null
        END AS age_bracket,
        CASE WHEN january_1st_age < 10 THEN 1
            WHEN january_1st_age >= 10 and january_1st_age < 20 then 2
            WHEN january_1st_age >= 20 and january_1st_age < 30 then 3
            WHEN january_1st_age >= 30 and january_1st_age < 40 then 4
            WHEN january_1st_age >= 40 and january_1st_age < 50 then 5
            WHEN january_1st_age >= 50 and january_1st_age < 60 then 6
            WHEN january_1st_age >= 60 and january_1st_age < 70 then 7
            WHEN january_1st_age >= 70 and january_1st_age < 80 then 8
            WHEN january_1st_age >= 80 and january_1st_age < 90 then 9
            WHEN january_1st_age > 89 then 10
            else null
        END AS
        age_bracket_sort_order
        FROM person
    )
   SELECT count(distinct person_id) as person_count, data_partner_id, gender_concept_name, age_bracket, age_bracket_sort_order FROM bracket
   group by data_partner_id, gender_concept_name, age_bracket, age_bracket_sort_order