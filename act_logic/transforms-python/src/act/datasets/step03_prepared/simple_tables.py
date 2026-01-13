from transforms.api import transform, Input, Output
from act.utils import add_site_id_col
from act.site_specific_utils import apply_site_parsing_logic
from pyspark.sql import functions as F, types as T
from act.anchor import path


def make_transform(domain):
    @transform(
        my_input=Input(path.transform + '02 - clean/' + domain),
        site_id_df=Input(path.site_id),
        processed=Output(path.transform + '03 - prepared/' + domain),
    )
    def compute_function(my_input, site_id_df, processed):
        processed_df = my_input.dataframe()

        # Add a "data_partner_id" column with the site's id to enable downstream primary key generation
        processed_df = add_site_id_col(processed_df, site_id_df)

        # Apply site-specific parsing logic (if applicable)
        processed_df = apply_site_parsing_logic(processed_df, site_id_df)

        if domain == "patient_dimension":
            # Add ethnicity_cd column if it is not present (optional ACT column)
            if "ethnicity_cd" not in processed_df.columns:
                processed_df = processed_df.withColumn("ethnicity_cd", F.lit(None).cast(T.StringType()))

        elif domain == "visit_dimension":
            processed_df = processed_df.withColumn(
                "VISIT_DIMENSION_ID",
                F.concat_ws("|", F.col("encounter_num"), F.col("patient_num"))
            )

        else:
            # No special logic
            pass

        processed.write_dataframe(processed_df)

    return compute_function


domains = ["concept_dimension", "control_map", "note", "note_nlp", "patient_dimension", "visit_dimension"]

transforms = (make_transform(domain) for domain in domains)
