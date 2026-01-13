from transforms.api import configure
from transforms.api import transform_df, Input, Output
from pyspark.sql.functions import expr

'''
Phenotype extraction script
The listed datasets in DOMAINS must be returned for downstream EPI/date shifts and desensitization steps
'''

DOMAINS = [
    "condition_era",
    "condition_occurrence",
    "death",
    "device_exposure",
    "drug_era",
    "drug_exposure",
    "measurement",
    "observation",
    "observation_period",
    "person",
    "procedure_occurrence",
    "visit_occurrence",
    "manifest_harmonized",
    "location",
    "measurements_to_macrovisits",
    "procedures_to_macrovisits",
    "conditions_to_macrovisit",
    "payer_plan_period",
    "provider",
    "care_site",
    "microvisits_to_macrovisits",
    "control_map",
    "note",
    "note_nlp",
    "visit_detail"
]


def transform_generator(domains):
    transforms = []

    for domain in domains:
        @configure(profile=['NUM_EXECUTORS_4'])
        @transform_df(
            Output("/UNITE/Phenotype Extraction - RWD Pipeline - Dev Datastream/datasets/cancer/cancer LDS/{domain}".format(domain=domain)),
            df=Input("/UNITE/Harmonization - RWD Pipeline - N3C COVID Replica/harmonized/{domain}".format(domain=domain)),
            persons_with_cancer=Input("ri.foundry.main.dataset.1de7526c-18e0-4182-8a5d-97b3d6c65798"),
            enclave_participation=Input("ri.foundry.main.dataset.d6674f53-0751-42ae-923d-c2608d76f29a")  # noqa: E501
        )
        def compute_function(df, persons_with_cancer, enclave_participation):
            if 'person_id' in df.columns:
                filtered_df = df.join(persons_with_cancer.select("person_id"), on="person_id", how="inner")
            elif 'data_partner_id' in df.columns:
                renal_enclaves = enclave_participation.filter(
                    expr("exists(enclaves, x -> lower(x) like '%cancer%')")
                ).select("data_partner_id")
                filtered_df = df.join(renal_enclaves, on="data_partner_id", how="inner")
            else:
                filtered_df = df
            return filtered_df

        transforms.append(compute_function)

    return transforms


TRANSFORMS = transform_generator(DOMAINS)
