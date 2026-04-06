def read_campaign_data(spark, input_path: str):
    return spark.read.csv(input_path, header=True, inferSchema=True)