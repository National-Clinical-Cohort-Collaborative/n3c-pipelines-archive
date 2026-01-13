CREATE TABLE `/UNITE/[RP-4A9E27] DI&H - Data Quality/DQD_Tests/excluded/removed_person` AS

    -- LDS removed logic have been moved into the pipeline
    -- create a union dataset of all of the removed dataset from each pipeline at the LDS level
    -- For each domain select the removed dataset
    -- union of all removed datasets are selected here in order to add to the site report card, each site's report card is filtered by site_id. 
    -- update the path to LDS where this is unioned form all sites.
    SELECT * FROM `ri.foundry.main.dataset.cf52dc55-9673-496d-86c0-3020beec9525`