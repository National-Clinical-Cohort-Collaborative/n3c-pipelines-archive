from transforms.api import transform_df, Input, Output, Markings, configure
from myproject.datasets import utils

primary_key_cols = [
    'care_site_id',
    'case_person_id',
    'condition_era_id',
    'condition_occurrence_id',
    'control_map_id',
    'control_person_id',
    'device_exposure_id',
    'dose_era_id',
    'drug_era_id',
    'drug_exposure_id',
    'location_id',
    'measurement_id',
    'note_id',
    'note_nlp_id',
    'observation_id',
    'observation_period_id',
    'payer_plan_period_id',
    'person_id',
    'preceding_visit_detail_id',
    'preceding_visit_occurrence_id',
    'procedure_occurrence_id',
    'provider_id',
    'visit_detail_id',
    'visit_detail_parent_id',
    'visit_occurrence_id',
]


def transform_generator(domains):
    transforms = []

    for domain in domains:
        @configure(profile=['NUM_EXECUTORS_32', 'EXECUTOR_MEMORY_LARGE'])
        @transform_df(
            Output("/UNITE/Safe Harbor/transform_staging/{domain}".format(domain=domain)),
            df=Input(
                "/UNITE/Safe Harbor/raw/{domain}".format(domain=domain),
                stop_propagating=Markings([
                    "2f2690b6-e4be-4873-852d-c2f1a6a61740",
                    "6c04089d-ae03-4ae5-b19b-a3912df4552e",
                    "032e4f9b-276a-48dc-9fb8-0557136ffe47",
                    ],
                    on_branches=utils.release_branches
                ),
            ),
            manifest=Input("ri.foundry.main.dataset.d87628fb-b9cd-449d-bddc-b463c272c9da", stop_propagating=Markings([
                    "2f2690b6-e4be-4873-852d-c2f1a6a61740",
                    "6c04089d-ae03-4ae5-b19b-a3912df4552e",
                    "032e4f9b-276a-48dc-9fb8-0557136ffe47",
                ],
                    on_branches=utils.release_branches
                ))
        )
        def compute_function(df, manifest, domain=domain):
            df = utils.release_by_data_partner_id(df, manifest)
            df = df.drop("payload")
            if domain == "observation":
                df = utils.suppress_value_as_string(df)

            for col in df.columns:  # noqa
                if col in primary_key_cols:
                    df = df.withColumn(col, df[col].cast("string"))

            partition_count = utils.domain_partitions[domain]
            return df.repartition(partition_count)

        transforms.append(compute_function)

    return transforms


TRANSFORMS = transform_generator(utils.DOMAINS)
