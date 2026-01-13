from transforms.api import transform, transform_df, Input, Output, configure
import datetime as dt
from myproject.datasets import utils
from myproject.datasets.phi_utils import clean_free_text_cols

# domain that will require clean_free_text_cols or other processing
relevant_dfs = [
    "condition_occurrence", 
    "death", 
    "measurement", 
    "observation", 
    "person", 
    "procedure_occurrence", 
    "provider", 
    "visit_occurrence"
]

# domains that will undergo simple identity transforms
DOMAINS = [
    "care_site",
    "condition_era",
    "control_map",
    "device_exposure",
    "drug_era",
    "drug_exposure",
    "location",
    "note",
    "note_nlp",
    "observation_period",
    "payer_plan_period",
    "visit_detail"
]

def transform_generator(domains_for_processing, identity_domains):
    transforms = []

    for domain in domains_for_processing:
        foundry_df_input = "/UNITE/Data Ingestion & OMOP Mapping/LDS Union/unioned_{domain}".format(domain=domain.lower())

        @configure(
            profile=utils.DOMAIN_PROFILES[domain.lower()],
            allowed_run_duration=dt.timedelta(hours=5),
            backend=utils.DOMAIN_BACKEND[domain.lower()]
        )
        @transform(
            processed=Output("/UNITE/LDS/clean/{domain}".format(domain=domain.lower())),
            nulled_rows=Output("/UNITE/LDS/cleaning_output/{domain}_nulled_rows".format(domain=domain.lower())),
            foundry_df=Input(foundry_df_input)
        )
        def compute_function(processed, nulled_rows, foundry_df, ctx, domain=domain):

            if domain.lower() == "measurement":
                ctx.spark_session.conf.set('spark.sql.shuffle.partitions', 2000)

            # Convert transform inputs to dataframes
            foundry_df = foundry_df.dataframe()

            # --------------------ALL domains
            # -----CLEAN FREE TEXT
            # Clean free text columns to address PHI and site-identifying information.
            p_key = utils.DOMAIN_PKEYS.get(domain.lower())

            foundry_df, nulled_rows_df = clean_free_text_cols(foundry_df, domain, p_key, ctx)
            # Log the rows that were nulled if this domain has any free text columns/any rows were flagged
            if nulled_rows_df:
                nulled_rows.write_dataframe(nulled_rows_df)

            if domain.lower() == "person":
                dedup_cols = ['global_person_id']
                omop_cols = [c for c in foundry_df.columns if c not in dedup_cols]
                foundry_df = foundry_df.select(*omop_cols, *dedup_cols)

            processed.write_dataframe(foundry_df)

        transforms.append(compute_function)

    for domain in identity_domains:
        @configure(profile=['NUM_EXECUTORS_4'])
        @transform_df(
            Output("/UNITE/LDS/clean/{domain}".format(domain=domain.lower())),
            df=Input("/UNITE/Data Ingestion & OMOP Mapping/LDS Union/unioned_{domain}".format(domain=domain.lower())),
        )
        def compute_function(df):
            return df

        transforms.append(compute_function)

    return transforms


TRANSFORMS = transform_generator(domains_for_processing=relevant_dfs, identity_domains=DOMAINS)