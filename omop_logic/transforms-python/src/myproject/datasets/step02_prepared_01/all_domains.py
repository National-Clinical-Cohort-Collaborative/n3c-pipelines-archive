# from pyspark.sql import functions as F
from transforms.api import configure, Input, Output, transform
from myproject.anchor import path
# from myproject.local_utils import perform_standard_mapping
from source_cdm_utils.standard_mapping import perform_standard_mapping
# from pyspark.sql import functions as F


def make_transform(domain, concept_col, profile):
    @configure(profile=profile)
    @transform(
        processed=Output(path.transform + "02 - prepared - 01/" + domain),
        my_input=Input(path.transform + "02 - clean/" + domain),
        concept=Input(path.concept),
        concept_relationship=Input(path.concept_relationship),
    )
    def compute_function(my_input, concept, concept_relationship, processed):
        processed_df, concept_df, concept_relationship_df = my_input.dataframe(), concept.dataframe(), concept_relationship.dataframe()

        if concept_col:
            processed_df = perform_standard_mapping(processed_df, concept_df, concept_relationship_df, concept_col, domain)

        processed.write_dataframe(processed_df)

    return compute_function


domains = [
    ("care_site", None, None),
    ("condition_era", "condition_concept_id", None),
    ("condition_occurrence", "condition_concept_id", None),
    ("control_map", None, None),
    ("death", None, None),
    ("device_exposure", "device_concept_id", None),
    ("dose_era", "drug_concept_id", None),
    ("drug_era", "drug_concept_id", None),
    ("drug_exposure", "drug_concept_id", None),
    ("location", None, None),
    ("measurement", "measurement_concept_id", "EXECUTOR_MEMORY_LARGE"),
    ("note", None, None), 
    ("note_nlp", "note_nlp_concept_id", "EXECUTOR_MEMORY_LARGE"),  # note_nlp_concept_id
    ("observation", "observation_concept_id", "EXECUTOR_MEMORY_MEDIUM"),
    ("observation_period", None, None),
    ("person", None, None),
    ("procedure_occurrence", "procedure_concept_id", None),
    ("provider", None, None),
    ("visit_detail", "visit_detail_id", None),
    ("visit_occurrence", "visit_concept_id", None)
]

transforms = (make_transform(*domain) for domain in domains)
