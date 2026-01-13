CREATE TABLE `ri.foundry.main.dataset.a4b28e69-f5d8-4398-895b-ec05d0bc6c69` TBLPROPERTIES (foundry_transform_profiles = 'NUM_EXECUTORS_4, EXECUTOR_MEMORY_MEDIUM') AS
  
   SELECT * FROM
    (
        -- union the two methods of joining to produce the final result
        SELECT * FROM `ri.foundry.main.dataset.a6bcb73f-ee56-436d-95dd-8b2221e491c9`
        UNION
        SELECT * FROM `ri.foundry.main.dataset.ea98a439-a108-4927-a937-0be222cd0ada`
    )
    
    
     