from transforms.api import configure, transform, Input, Output
from pcornet.anchor import path
from source_cdm_utils import pre_clean


def make_transform(domain, profile):
    @configure(profile=profile)
    @transform(
        processed=Output(path.transform + "07 - pre clean/processed/" + domain),
        nulled_rows=Output(path.transform + "07 - pre clean/nulled/" + domain),
        removed_rows=Output(path.transform + "07 - pre clean/removed/" + domain),

        foundry_df=Input(path.transform + "06 - id generation/" + domain),
        removed_person_ids=Input(path.transform + "07 - pre clean/pre_clean_removed_person_ids"),

        ahrq_xwalk=Input("ri.foundry.main.dataset.4540b6c9-cbb2-45f7-8c08-35e24b27e0c8"),
        tribal_zips=Input("ri.foundry.main.dataset.29c4419f-56ed-48af-8038-7178f27c3acc"),
        loincs_to_remove=Input("ri.foundry.main.dataset.0895fb38-6c41-4146-a30f-9213c78f6e80")
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
    ("care_site", None),
    ("condition_era", None),
    ("condition_occurrence", None),
    ("control_map", None),
    ("death", None),
    ("device_exposure", None),
    # dose_era
    ("drug_era", None),
    ("drug_exposure", None),
    ("location", None),
    ("measurement", ["EXECUTOR_MEMORY_LARGE"]),
    ("note", None),
    ("note_nlp", None),
    ("observation", None),
    ("observation_period", None),
    ("payer_plan_period", None),
    ("person", None),
    ("procedure_occurrence", None),
    ("provider", None),
    ("visit_occurrence", None),
    ("visit_detail", None)
]

transforms = (make_transform(*domain) for domain in domains)
