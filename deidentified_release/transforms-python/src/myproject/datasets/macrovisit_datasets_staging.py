from transforms.api import configure, transform_df, Input, Output, Markings
from myproject.datasets import utils

primary_key_cols = [
    'care_site_id',
    'condition_era_id',
    'condition_occurrence_id',
    'dose_era_id',
    'drug_era_id',
    'drug_exposure_id',
    'location_id',
    'measurement_id',
    'observation_id',
    'observation_period_id',
    'person_id',
    'procedure_occurrence_id',
    'provider_id',
    'visit_occurrence_id',
    'payer_plan_period_id'
]


def transform_generator(datasets):
    transforms = []

    for dataset in datasets:

        @configure(profile=['NUM_EXECUTORS_16', 'EXECUTOR_MEMORY_MEDIUM', 'EXECUTOR_MEMORY_OVERHEAD_MEDIUM'])
        @transform_df(
            Output("/UNITE/Safe Harbor/transform_staging/{dataset}".format(dataset=dataset)),
            my_input=Input(
                    "/UNITE/Safe Harbor/raw/{dataset}".format(dataset=dataset),
                    stop_propagating=Markings(
                            [
                        "2f2690b6-e4be-4873-852d-c2f1a6a61740",
                        "6c04089d-ae03-4ae5-b19b-a3912df4552e",
                        "032e4f9b-276a-48dc-9fb8-0557136ffe47",
                    ],
                            on_branches=utils.release_branches
                    ),
                ),
            manifest=Input("ri.foundry.main.dataset.d87628fb-b9cd-449d-bddc-b463c272c9da",
                    stop_propagating=Markings(
                            [
                        "2f2690b6-e4be-4873-852d-c2f1a6a61740",
                        "6c04089d-ae03-4ae5-b19b-a3912df4552e",
                        "032e4f9b-276a-48dc-9fb8-0557136ffe47",
                    ],
                            on_branches=utils.release_branches
                    ),
                
                ),
        )
        def my_compute_function(my_input, manifest):
            df = my_input
            df = utils.release_by_data_partner_id(df, manifest)

            # turn all id columns to string
            for col in df.columns:
                if col in primary_key_cols:
                    df = df.withColumn(col, df[col].cast("string"))


            df = df.drop("payload")
            partition_count = utils.dataset_partitions[dataset]
            return df.repartition(partition_count)

        transforms.append(my_compute_function)

    return transforms

TRANSFORMS = transform_generator(utils.DATASETS_TO_STAGE)
