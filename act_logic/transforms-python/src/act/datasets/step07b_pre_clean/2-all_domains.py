from transforms.api import transform, Input, Output
from act.anchor import path
from source_cdm_utils import pre_clean


def make_transform(domain):
    @transform(
        processed=Output(path.transform + "07 - pre clean/processed/" + domain),
        nulled_rows=Output(path.transform + "07 - pre clean/nulled/" + domain),
        removed_rows=Output(path.transform + "07 - pre clean/removed/" + domain),

        foundry_df=Input(path.transform + "06 - id generation/" + domain),
        removed_person_ids=Input(path.transform + "07 - pre clean/pre_clean_removed_person_ids"),

        ahrq_xwalk=Input("ri.foundry.main.dataset.43dd4c5b-07b3-4158-8e01-13ea3d32fdaa"),
        tribal_zips=Input("ri.foundry.main.dataset.bfe43542-9f20-4275-85ec-e44d6e9d5544"),
        loincs_to_remove=Input("ri.foundry.main.dataset.8acbca23-d3ae-4ac3-a25d-85d7c11c65e0")
    )
    def compute_function(
        processed, nulled_rows, removed_rows,
        foundry_df, removed_person_ids,
        ahrq_xwalk, tribal_zips, loincs_to_remove, ctx
    ):
        pre_clean.do_pre_clean(
            domain,
            processed, nulled_rows, removed_rows,
            foundry_df, removed_person_ids,
            ahrq_xwalk, tribal_zips, loincs_to_remove, ctx)

    return compute_function


domains = [
    # "care_site",
    "condition_era",
    "condition_occurrence",
    "control_map",
    "death",
    "device_exposure",
    # dose_era
    "drug_era",
    "drug_exposure",
    "location",
    "measurement",
    "note",
    "note_nlp",
    "observation",
    "observation_period",
    # "payer_plan_period",
    "person",
    "procedure_occurrence",
    # "provider",
    "visit_occurrence",
    "visit_detail"
]

transforms = (make_transform(domain) for domain in domains)
