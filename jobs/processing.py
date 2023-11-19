from pyspark.sql import SparkSession
from pyspark import SparkConf
import os
from pathlib import Path
import configparser
from constants import data_path
from lib.log4j import Log4J

path = Path(__file__)
ROOT_DIR = path.parent.absolute()
config_path = os.path.join(ROOT_DIR, "../spark.conf")


# Processing functions, this could be its own python class
def get_spark_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read(config_path)

    print(config.items())

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def load_survey(spark, data_file):
    print(data_file)
    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)

    return survey_df


def filter_survey(survey_df):
    return survey_df \
        .where("Age < 40") \
        .select("Age", "Gender", "Country", "State")


def count_survey(filtered_survey_df):
    grouped_df = filtered_survey_df.groupBy("Country")
    return grouped_df.count()


def count_survey_using_spark_sql(spark, survey_df):
    survey_df.createOrReplaceTempView("survey_table")
    count_by_country_df = spark.sql("Select Country, count(1) as count from survey_table"
                                    " where Age < 40 group by country")
    return count_by_country_df


if __name__ == "__main__":
    conf = get_spark_config()
    spark = (SparkSession.builder
             .config(conf=conf)
             .getOrCreate())

    logger = Log4J(spark)
    logger.info("Starting PySpark ETL job")

    # Read or Extract Stage
    survey_df = load_survey(spark, data_path) \
        .repartition(2)

    # Transform Stage using Dataframe Approach
    # Transform Stage
    logger.info("Transforming using dataframe approach")
    survey_filter_df = filter_survey(survey_df)

    country_count_df_approach1 = count_survey(survey_df)

    # Transform Stage using Spark Sql Approach
    logger.info("Transforming using spark sql approach")
    country_count_df_approach2 = count_survey_using_spark_sql(spark, survey_df)

    # Completion stage
    country_count_df_approach1.show()
    country_count_df_approach2.show()

    logger.info("Finish PySpark ETL job")

    spark.stop()
