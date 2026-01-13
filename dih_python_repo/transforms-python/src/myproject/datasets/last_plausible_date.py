from pyspark.sql import functions as F
from transforms.api import transform_df, Input, Output
from pyspark.sql.functions import datediff
import pandas as pd  # noqa
import datetime
from pyspark.sql import SparkSession


@transform_df(
    Output("/UNITE/[RP-4A9E27] DI&H - Data Quality/datasets/manifest_clean_with_last_plausible_date"),
    source_df=Input("ri.foundry.main.dataset.33088c89-eb43-4708-8a78-49dd638d6683"),
)
def compute(source_df, ctx):
    return create_last_plausible_date(source_df, ctx)


def create_last_plausible_date(manifest, ctx):
    df = manifest.withColumn("data_submit_date", F.coalesce(F.col("run_date"), F.col("contribution_date")))
    ########################################################################################
    ### Part 1. when data_partners did not provide thier run date or contribution dates
    ###          -> make data_sumbit_date into Max date out of other data partners.
    # Make max date into list 1x1 that's why using [0][0] -> so make max date into object.
    date_max = df.agg({"data_submit_date": "max"}).collect()[0][0]  # noqa
    df_sumbit_date = df.withColumn("data_submit_date_no_null_temp",
                                   F.when(df.data_submit_date.isNull(), date_max).otherwise(F.col("data_submit_date")))
    #######################################################################################
    ### part 2. Consider shifted date and expand the data_sumbit date.
    df_shift = manifest
    df_temp = df_shift.toPandas()  # covert into Pandas data frame.
    df_temp["max_num"] = df_temp.max_num_shift_days.str.extract('(\d+)')  # extract only numbers from the column "max_num_shift_days"
    df_temp["max_num"] = df_temp["max_num"].fillna(value=0)
    df_temp["sign"] = df_temp["max_num"]  # create sign column -> when we add days on the last step, we are going to decide based on what they report.
    # basic logic here "shift_date_yn" == "Y" -> we grab everything on the left of the numbers from "max_num_shift_days" column
    for i in range(df_temp.shape[0]):
        if df_temp["shift_date_yn"][i] == 'N':
            df_temp["sign"][i] = None
        elif df_temp["shift_date_yn"][i] == 'Y':  # some report as X.
            df_temp["sign"][i] = df_temp["max_num_shift_days"][i][:(df_temp["max_num_shift_days"][i]).index(df_temp["max_num"][i])] # extract all string before the number
            df_temp["sign"] = df_temp["sign"].str.strip() ## remove space in the sign column
    df_temp_last = df_temp[["data_partner_id", "max_num", "sign"]] ## select only data_partner_id and max-shift date and sign. 

    df_temp_last["max_num"] = df_temp_last["max_num"].astype(str)
    df_temp_last["sign"] = df_temp_last["sign"].astype(str)
    df_shift_final = ctx.spark_session.createDataFrame(df_temp_last) # make it into spark dataframe. 
    df_shift_final = df_shift_final.withColumn("max_num_shift", F.when(F.col("max_num").isNotNull(),
                                                                       F.col("max_num").cast("integer"))
                                                                 .otherwise(0)
                                             ).select("data_partner_id", "max_num_shift", "sign")

########################################################################################
    ### part 3. Joining shfted date and expand / reduce data_sumbit_date and make last_plausible_date
    df_expand = df_sumbit_date.join(df_shift_final, on = "data_partner_id", how ="left")
    df_expand_temp = df_expand.toPandas()

    for i in range(df_expand_temp.shape[0]):
        if df_expand_temp["sign"][i] == '-': 
            ## If the data partner reports only negative numbers, do not shift from given date
            df_expand_temp.loc[i, 'data_submit_date_no_null'] = df_expand_temp.loc[i, 'data_submit_date_no_null_temp']
            #df_expand_temp.loc[i, 'data_submit_date_no_null'] = df_expand_temp.loc[i, 'data_submit_date_no_null_temp'] - datetime.timedelta(days=int(0)) # maximum 14 days shifting, so some might filterd out if a patient shifted less than 14. 
        else:
            ## Otherwise add shifted amount to data_partner date
            df_expand_temp.loc[i, 'data_submit_date_no_null'] = df_expand_temp.loc[i, 'data_submit_date_no_null_temp'] + datetime.timedelta(days=int(df_expand_temp.loc[i,'max_num_shift']))
    ### Finally make it into 
    ### Add "transform_run_date" on the dataset. 
    df_expand_temp['transform_run_date'] = pd.Timestamp.today().strftime('%Y-%m-%d')

    df_expand_temp_last = df_expand_temp[["data_partner_id", "data_submit_date_no_null", 'transform_run_date']] ## select only data_partner_id and max-shift date. 
    df_expand_temp_last = pd.DataFrame(df_expand_temp_last,
               columns =["data_partner_id", "data_submit_date_no_null", 'transform_run_date']) # create data frame.
    df_expand_final = ctx.spark_session.createDataFrame(df_expand_temp_last) # make it into spark data frame. 


    ########################################################################################
    ### part 4. Join expanded_date and update the data_sumbit_date
    df_final = df_expand.join(df_expand_final, on = "data_partner_id", how= "left").drop("data_submit_date_no_null_temp")
    df_final = df_final.withColumnRenamed("data_submit_date_no_null", "last_plausible_date").drop("data_submit_date", "max_num_shift", "sign")

    ########################################################################################
    ### part 5. Add update info. 
    df_final = df_final.withColumn("Data_update_info", F.when(F.col("run_date").isNull(), "No_information_about_update")
                                                        .when( datediff(F.col("transform_run_date"), F.col("run_date") ) > 180, "Not_updated_for_past_6_months" )
                                                        .otherwise("Up_to_date_within_the_past_6_months")
                                            )
                                            
    return df_final

#################################################
## Global imports and functions included below ##
#################################################

