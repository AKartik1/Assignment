from pyspark.sql.functions import sum

def aggregate_campaign_triage(df):
    return df.groupBy("campaign_id", "platform_std", "event_dt") \
        .agg(
            sum("ad_spend").alias("total_spend"),
            sum("clicks").alias("total_clicks"),
            sum("impressions").alias("total_impressions"),
            sum("conversions").alias("total_conversions")
        )