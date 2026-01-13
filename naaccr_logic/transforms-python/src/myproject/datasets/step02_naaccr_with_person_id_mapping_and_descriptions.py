from pyspark.sql import types as T
from pyspark.sql import functions as F
from transforms.api import transform, Input, Output, Check
from transforms.verbs.dataframes import union_many
from pyspark.sql.types import StringType
from transforms import expectations as E
import logging

"""
OVERVIEW
Join NAACR data with site person ID to N3C ID mappings from site pipelines 
/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: PCORnet/Site X/union_staging/staged/person

Col descriptions added from data dictionary using col metadata approach
    https://www.palantir.com/docs/foundry/transforms-python/output-column-metadata#example-write-column-descriptions-in-code-repository-transforms
Col descriptions are truncated at a max len of 800 chars
"""

inputs = {
    "349": Input(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: PCORnet/Site 349/union_staging/staged/person"
    ),
    "179": Input(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: OMOP/Site 179/union_staging/staged/person"
    ),
    "126": Input(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: PCORnet/Site 126/union_staging/staged/person"
    ),
    "220": Input(
        "/UNITE/Data Ingestion & OMOP Mapping - RWD Pipeline - N3Clinical/Source Data Model: PCORnet/Site 220/union_staging/staged/person"
    ),
}

logger = logging.getLogger()


@transform(
    **inputs,
    naaccr_output_df=Output(
        "ri.foundry.main.dataset.fcc4f44f-ecb4-4005-b9ef-22009bfdbbe6",
        checks=Check(
            E.col("person_id").non_null(),
            "Check for non-null person_id",
            on_error="FAIL",
        ),
    ),
    naaccr_person_mapping_df=Output(
        "ri.foundry.main.dataset.650a3b9c-d05e-4167-904a-ba0712282593"
    ),
    naaccr_input_df=Input(
        "ri.foundry.main.dataset.b1bd417f-c469-4019-90b5-975f7358918e"
    ),
    naaccr_data_dictionary=Input(
        "ri.foundry.main.dataset.615fb648-7f0d-40e7-b2ca-78c2ebc553a0"
    ),
)
def compute(
    naaccr_input_df,
    naaccr_output_df,
    naaccr_person_mapping_df,
    naaccr_data_dictionary,
    ctx,
    **all_person_dfs,
):
    # create a schema dict and column description dict from the data dictionary
    naaccr_input_df = naaccr_input_df.dataframe()
    naaccr_cols = naaccr_input_df.columns

    # Select columns and collect data
    collected_data = (
        naaccr_data_dictionary.dataframe()
        .select("Data_Item_Number", "Description")
        .collect()
    )

    # Convert to dictionary
    data_type_dict = {
        row["Data_Item_Number"]: row["Description"] for row in collected_data
    }
    schema_dict = {f"N{key}": value for key, value in data_type_dict.items()}
    schema_dict["N3C_PATIENT_ID"] = T.StringType()

    # Select keys that pertain to items in the list, maintaining the order
    selected_keys = [key for key in schema_dict if key in naaccr_cols]

    # Create a new dictionary with the selected keys
    n3c_naaccr_schema_long_vals = {key: schema_dict[key] for key in selected_keys}

    # Truncate values to 800 characters
    n3c_naaccr_schema = {
        key: str(value)[:800] for key, value in n3c_naaccr_schema_long_vals.items()
    }

    # ensure relevant columns are selected from the person tables and correct DataTypes where needed
    person_dfs = [df.dataframe() for df in all_person_dfs.values()]

    logging.info(all_person_dfs.values())

    # ensure all unioned person tables are in the right format to union
    corrected_person_dfs = []
    for person_df in person_dfs:
        if "site_patid" in person_df.columns:  # PCORnet
            df = person_df.select(
                "person_id", "site_patid", "data_partner_id"
            ).distinct()
            corrected_person_dfs.append(df)
        elif "site_person_id" in person_df.columns:  # OMOP
            df = (
                person_df.select("person_id", "site_person_id", "data_partner_id")
                .distinct()
                .withColumn(
                    "site_person_id", person_df["site_person_id"].cast(StringType())
                )
                .withColumnRenamed("site_person_id", "site_patid")
            )
            corrected_person_dfs.append(df)

    person_id_mapping = union_many(*corrected_person_dfs, how="strict")  # noqa: C416

    naaccr_input_df = naaccr_input_df.join(
        person_id_mapping,
        (naaccr_input_df.N3C_PATIENT_ID == person_id_mapping.site_patid)
        & (naaccr_input_df.data_partner_id == person_id_mapping.data_partner_id),
        "left",
    ).drop(
        naaccr_input_df.data_partner_id
    )  # "N3C_PATIENT_ID",

    # Rearrange 'data_partner_id' in the list
    if "data_partner_id" in naaccr_cols:
        # Remove 'data_partner' from its current position
        naaccr_cols.remove("data_partner_id")
        # Insert 'data_partner' as the second column
        naaccr_cols.insert(1, "data_partner_id")

    naaccr_cols.remove("N3C_PATIENT_ID")
    # Create the output columns list
    output_cols_naaccr = ["person_id"] + naaccr_cols
    output_cols_id_mapping = ["person_id", "site_patid", "data_partner_id"]

    naaccr_out = naaccr_input_df.select(*output_cols_naaccr).filter(
        F.col("person_id").isNotNull()
    )
    mapping_out = naaccr_input_df.select(*output_cols_id_mapping).filter(
        F.col("person_id").isNotNull()
    )

    naaccr_output_df.write_dataframe(naaccr_out, column_descriptions=n3c_naaccr_schema)
    naaccr_person_mapping_df.write_dataframe(mapping_out)

