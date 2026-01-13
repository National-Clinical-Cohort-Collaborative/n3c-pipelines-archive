from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/UNITE/[PPRL] Centers for Medicare & Medicaid Services (CMS) Release/datasets/datasets/n3c_medicare_medicaid_gpi_mapping_table"),
    n3c_to_medicare=Input("ri.foundry.main.dataset.d7a0b290-0aad-43c6-8aee-99851b49561b"),
    n3c_to_medicaid=Input("ri.foundry.main.dataset.ae72ccc0-b081-41a8-acf6-24eea9a71b21"),
    n3c_to_gpi=Input("ri.foundry.main.dataset.28a81d33-1e43-468d-8217-ef44c7f8fc32"),
)
def compute(n3c_to_medicare, n3c_to_medicaid, n3c_to_gpi):
    n3c_to_medicaid = n3c_to_medicaid.withColumnRenamed("cms_person_id", "medicaid_person_id")
    n3c_to_medicare = n3c_to_medicare.withColumnRenamed("cms_person_id", "medicare_person_id")

    all_cms_n3c = n3c_to_medicare.join(n3c_to_medicaid, on=["n3c_person_id", "data_partner_id"], how="full")

    n3c_to_gpi = n3c_to_gpi.select("person_id", "global_person_id")
    final_mapping = all_cms_n3c.join(n3c_to_gpi, F.col("n3c_person_id") == F.col("person_id"), how="left"). drop(
        "person_id", "data_partner_id")
    final_mapping = final_mapping.filter((
        F.col('global_person_id').isNotNull()) & (F.col('global_person_id') != 'No Duplicate or Linkage Found'))

    return final_mapping
