from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output


@transform_df(
    Output("/UNITE/[RP-4A9E27] DI&H - Data Quality/datasets/creatitine_histograms"),
    person=Input("ri.foundry.main.dataset.456df227-b15d-4356-9df6-b113e90eb239"),
    unique_days=Input("ri.foundry.main.dataset.92641406-8e61-479f-bdcf-e157de37162a")
)
def compute(person, unique_days):
    # prep for direct histogram graphing by backfilling zeros
    # normalize data to percent of total population in each bin    
    person = person.select("person_id","data_partner_id")
    unique_days = person.join(unique_days,["person_id","data_partner_id"],"left")
    unique_days = unique_days.fillna({'unique_days': 0})

    site_person_counts = person.groupBy("data_partner_id").agg(F.countDistinct("person_id")
                                                    .cast("integer").alias("total_persons"))

    hist_data = unique_days.groupBy("unique_days","data_partner_id").agg(F.countDistinct("person_id")
                                                    .cast("integer").alias("person_count"))
    hist_data = hist_data.join(site_person_counts,"data_partner_id","left")
    hist_data = hist_data.withColumn("person_count",F.col("person_count")/F.col("total_persons"))
    hist_data = hist_data.drop("total_persons")

    list_of_days = hist_data.select("unique_days").distinct()
    hist_data = list_of_days.join(hist_data,"unique_days","left")
    hist_data = hist_data.fillna({'person_count': 0})
    return hist_data
