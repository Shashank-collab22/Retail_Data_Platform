import pyspark.sql.functions as F
from common_utils.logging import get_logger
logger = get_logger("common_utils_bronze")


def read_raw(spark,raw_path,file_format,options=None):
    logger.info("reading from path :%s",raw_path)
    logger.info("File format: %s",file_format)
    reader=spark.read.format(file_format)
    for key,value in (options or {}).items():
        reader= reader.option(key,value)
    return reader.load(raw_path)
    logger.info("read completed and stored in df")






def bronze_ingestor(df,mode,target_table):

    """
    this function will add audit coloumn and write in bronze as delta table
    df: dataframe read from raw volume
    mode: overwrite or append
    target_table: catlog.schema table """
    logger.info("adding audit columnsin dataframe")

    df=df.withColumn("last_update_ts",F.current_timestamp())\
    .withColumn("File_path",F.col("_metadata.file_path")) 


    logger.info("writing to delta table%s",target_table)
    logger.info("selected mode is %s",mode)
    


    df.write.format("delta").mode(mode).saveAsTable(target_table)
    return df.count()