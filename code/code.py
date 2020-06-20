from functions import * 
from pyspark.sql import functions as F


if __name__ == "__main__":
    # spark configuration
    spark = SparkConfig()
    
    # create custom schema based on a json file 
    json_file = "config/types_mapping.json"
    schema  = CustomSchema(json_file)
    
    # CSV file location and type
    csv_file_location = "data/input/users/load.csv"
    file_type = "csv"

    # CSV options
    # Is possible to inferSchema using the function of spark, but the challenge is create a custom schema. 
    infer_schema = "false"
    first_row_is_header = "true"
    delimiter = ","
    
    # Create the dataframe reading the csv file and custom schema
    df = spark.read.format(file_type).option("header", first_row_is_header).option("sep",delimiter).schema(schema).load(csv_file_location)
    
    if df.filter(F.col('name').contains('@')):
            df = df.withColumn('aux_email', df['name']).withColumn('name', df['email'])
            df = df.withColumn('email', df['aux_email']).drop('aux_email')
    
    # Drop all records that are duplicates         
    df_final = df.orderBy('id', 'update_date', ascending= False).dropDuplicates(subset=['id'])
    
    # Writing the final dataframe in parquet inside of output folder
    parquet_file = "data/output/load.parquet"
    df_final.write.parquet(parquet_file)
    
    # Show the final dataframe 
    df_final.orderBy('id').show()