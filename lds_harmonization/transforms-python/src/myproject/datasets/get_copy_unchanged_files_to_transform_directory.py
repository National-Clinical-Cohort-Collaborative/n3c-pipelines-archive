from transforms.api import configure
from transforms.api import transform_df, Input, Output


DOMAINS = [
    "care_site",
    "condition_era",
    "condition_occurrence",
    "control_map",
    "death",
    "device_exposure",
    "drug_era",
    "drug_exposure",
    "location",
    "note",
    "note_nlp",
    "observation",
    "observation_period",
    "payer_plan_period",
    "person",
    "procedure_occurrence",
    "provider",
    "visit_detail"
]


def transform_generator(domains):
    transforms = []

    for domain in domains:
        @configure(profile=['NUM_EXECUTORS_4'])
        @transform_df(
            Output("/UNITE/LDS/harmonized/{domain}".format(domain=domain)),
            df=Input("/UNITE/LDS/clean/{domain}".format(domain=domain)),
        )
        def compute_function(df):
            return df

        transforms.append(compute_function)

    return transforms


TRANSFORMS = transform_generator(DOMAINS)
