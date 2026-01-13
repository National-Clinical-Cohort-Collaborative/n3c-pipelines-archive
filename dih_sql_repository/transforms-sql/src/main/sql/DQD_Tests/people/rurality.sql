CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/rurality` AS

-- Note: it was not counting people with null results accurately, hence the nesting
WITH totals_by_rurality AS
(  
SELECT t.data_partner_id,t.rurality, (t.number_of_people + t.number_null_zips) AS number_of_people
FROM (
    SELECT z.data_partner_id, z.rurality, COUNT(z.*) AS number_of_people, sum(case when z.zip is null then 1 else 0 end) number_null_zips
    FROM (  
        SELECT p.data_partner_id, l.zip, r.RUCA1 as ruca, 
            CASE WHEN r.RUCA1 BETWEEN 1 AND 3 THEN 'Urban'
            WHEN r.RUCA1 BETWEEN 4 AND 10 THEN 'Rural'
            ELSE 'Missing'
            END AS rurality
        FROM `/UNITE/LDS/clean/person` p
        LEFT JOIN `/UNITE/LDS/clean/location` l
            ON p.location_id = l.location_id
        LEFT JOIN `/UNITE/[EXTDATASET-59] RUCA Rural-Urban Commuting Area Codes/RUCA2010zipcode/RUCA2020zipcode` r
            ON l.zip = r.ZIP_CODE
        ) AS z
    GROUP BY z.data_partner_id, z.rurality
    ) AS t
ORDER BY t.data_partner_id, t.rurality

)


SELECT  r.*,
        t.number_of_people AS total_people,
        ROUND((r.number_of_people * 100 / t.number_of_people), 2) AS percent_people
FROM totals_by_rurality r
LEFT JOIN `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/people/people_per_site` t
ON r.data_partner_id = t.data_partner_id
ORDER BY data_partner_id, rurality