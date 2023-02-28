import configparser
import os
from pyspark.sql import SparkSession
from config import LocalConfigurations, RemoteConfigurations
from cleaner import *
from pyspark.sql.functions import col, monotonically_increasing_id, lit, split
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, to_timestamp
from schemas import demographics_schema, airport_schema, world_temp_schema, immigrations_schema

cloud_config = configparser.ConfigParser()
cloud_config.read('cloud.cfg')
config = LocalConfigurations() if cloud_config['OTHER']['USE_LOCAL'] == 'true' else RemoteConfigurations()

def create_spark_session():
    '''
    Creates spark session.

          Returns (SparkSession):
                    The spark session
    '''
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:3.0.0-s_2.12") \
        .getOrCreate()
    spark.conf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    return spark


def save_parquet(df, save_dir, partition_columns):
    '''
    Saves table to a parquet file.

            Parameters:
                    df (pyspark.sql.DataFrame): The dataframe that should be saved to parquet file
                    save_dir (string): The path to the directory that the parquet files should be saved
                    partition_columns (List[string]): The partition columns.
    '''
    df.write.mode('overwrite').partitionBy(*partition_columns).parquet(save_dir)


def read_demographics_data(spark):
    '''
    Read demographic data into spark dataframe.

            Parameters:
                    spark (SparkSession): A spark session

            Returns (pyspark.sql.DataFrame):
                    Original demographic data
    '''
    df = spark.read.options(header='True', delimiter=';') \
        .csv(config.DEMOGRAPHICS_DATA_PATH, schema=demographics_schema)
    return df


def read_temperature_data(spark):
    '''
    Read temperature data into spark dataframe.

            Parameters:
                    spark (SparkSession): A spark session

            Returns (pyspark.sql.DataFrame):
                    Original temperature data
    '''
    df = spark.read.options(header='True', delimiter=',') \
        .csv(config.TEMPERATURE_DATA_PATH, schema=world_temp_schema)
    return df


def read_immigration_data_april(spark):
    '''
    Read immigration data into spark dataframe, only from April.

            Parameters:
                    spark (SparkSession): A spark session

            Returns (pyspark.sql.DataFrame):
                    Original immigration data
    '''
    print(os.path.join(config.SAS_DATA_PATH, 'i94_apr16_sub.sas7bdat'))
    df = spark.read.format('com.github.saurfang.sas.spark').load(
        os.path.join(config.SAS_DATA_PATH, 'i94_apr16_sub.sas7bdat'), schema=immigrations_schema)
    return df


def read_immigration_data(spark):
    '''
    Read immigration data into spark dataframe.

            Parameters:
                    spark (SparkSession): A spark session

            Returns (pyspark.sql.DataFrame):
                    Original immigration data
    '''
    df = spark.read.option('recursiveFileLookup', 'true').format('com.github.saurfang.sas.spark').load(config.SAS_DATA_PATH, schema=immigrations_schema)
    return df


def read_port_data(spark):
    '''
    Read port data into spark dataframe.

            Parameters:
                    spark (SparkSession): A spark session

            Returns (pyspark.sql.DataFrame):
                    Original port data
    '''
    df = spark.read.options(header='True', delimiter=',') \
        .csv(config.AIRPORT_DATA_PATH, schema=airport_schema)
    return df


def pre_augment_immigration_data(df_immigration):
    '''
    Pre-augment immigration data for simpler processing

            Parameters:
                    df_immigration (pyspark.sql.DataFrame): Original immigration dataframe

            Returns (pyspark.sql.DataFrame):
                    Augmented immigration data
    '''
    '''Pre-augment immigration data for simpler processing'''
    df_immigration_aug = df_immigration \
        .withColumn('arrdate', to_date_sas('sas_arrdate')) \
        .withColumn('depdate', to_date_sas('sas_depdate')) \
        .withColumn('transport_no', to_transport_no('fltno', 'airline', 'i94mode')) \
        .withColumn('city', city_sas(df_immigration.i94port)) \
        .withColumn('state', state_sas(df_immigration.i94port)) \
        .na.drop(subset=["admnum"])
    return df_immigration_aug


def process_city_data(spark, immigration_aug_df):
    '''
    Process city data into the correct format.

            Parameters:
                    spark (SparkSession): A spark session
                    immigration_aug_df (pyspark.sql.DataFrame): Augmented immigration table

            Returns (pyspark.sql.DataFrame):
                    City data
    '''
    df_demographic = read_demographics_data(spark)
    df_temperature = read_temperature_data(spark)
    # 1. Cleaning demographic data
    #      1.1 Fill NaNs and remove duplicated rows if exists
    col_names = ['male_population', 'female_population', 'number_of_veterans', 'foreign_born']
    df_demographic_structured = df_demographic \
        .na.fill(value=0, subset=col_names) \
        .dropDuplicates()

    df_demographic_structured.printSchema()

    # 1. Cleaning temperature data
    #      2.1 Average Temperature in the US
    df_temperature_structured = df_temperature \
        .select('city', 'country', 'average_temperature') \
        .where(df_temperature.country == 'United States') \
        .groupBy('city', 'country').mean() \
        .withColumnRenamed('avg(average_temperature)', 'avg_temp') \
        .drop('country')

    # 3. Reconstruction
    #      3.1 - Find most common race for each city-state pair.
    df_demographic_structured.createOrReplaceTempView("demographic_race_count")
    df_demographic_count = spark.sql("select city, state, count from demographic_race_count") \
        .groupby('city', 'state') \
        .max() \
        .withColumnRenamed('max(count)', 'count')
    df_demographic_structured = df_demographic_structured \
        .join(df_demographic_count, on=['city', 'state', 'count'], how='inner') \
        .withColumnRenamed('race', 'most_common_race') \
        .withColumnRenamed('city', 'name') \
        .withColumn('name', upper(col('name'))) \
        .withColumn('country', lit('US')) \
        .drop('count')

    df_demographic_temp = df_demographic_structured \
        .join(df_temperature_structured, df_demographic_structured.name == df_temperature_structured.city, "left") \
        .drop('city')

    df_demographic_temp = df_demographic_temp \
        .join(immigration_aug_df.select(col('state').alias('state_code'), col('city').alias('name')).dropDuplicates(),
              ['state_code', 'name'], "fullouter") \
        .withColumn('city_id', monotonically_increasing_id())
    return df_demographic_temp


def process_time_data(immigration_aug_df):
    '''
    Process time data into the correct format.

            Parameters:
                    immigration_aug_df (pyspark.sql.DataFrame): Augmented immigration dataframe

            Returns (pyspark.sql.DataFrame):
                    Time data
    '''
    arr_time_df = immigration_aug_df.select(
        to_timestamp_sas(immigration_aug_df.sas_arrdate).alias('time'),
        hour(immigration_aug_df.arrdate).alias('hour'),
        dayofmonth(immigration_aug_df.arrdate).alias('day'),
        weekofyear(immigration_aug_df.arrdate).alias('week'),
        month(immigration_aug_df.arrdate).alias('month'),
        year(immigration_aug_df.arrdate).alias('year'),
        dayofweek(immigration_aug_df.arrdate).alias('weekday')
    )
    dep_time_df = immigration_aug_df.select(
        to_timestamp_sas(immigration_aug_df.sas_depdate).alias('time'),
        hour(immigration_aug_df.depdate).alias('hour'),
        dayofmonth(immigration_aug_df.depdate).alias('day'),
        weekofyear(immigration_aug_df.depdate).alias('week'),
        month(immigration_aug_df.depdate).alias('month'),
        year(immigration_aug_df.depdate).alias('year'),
        dayofweek(immigration_aug_df.depdate).alias('weekday')
    )

    expiration_time_df = immigration_aug_df.select(
        to_timestamp(immigration_aug_df.dtaddto).alias('time'),
        hour(immigration_aug_df.dtaddto).alias('hour'),
        dayofmonth(immigration_aug_df.dtaddto).alias('day'),
        weekofyear(immigration_aug_df.dtaddto).alias('week'),
        month(immigration_aug_df.dtaddto).alias('month'),
        year(immigration_aug_df.dtaddto).alias('year'),
        dayofweek(immigration_aug_df.dtaddto).alias('weekday')
    )
    expiration_time_df.head()
    time_df = arr_time_df \
        .union(dep_time_df) \
        .union(expiration_time_df) \
        .dropDuplicates() \
        .na.drop()
    return time_df


def process_port_data(spark, immigration_aug_df):
    '''
    Process port data into the correct format.

            Parameters:
                    spark (SparkSession): A spark session
                    immigration_aug_df (pyspark.sql.DataFrame): Augmented immigration dataframe

            Returns (pyspark.sql.DataFrame):
                    Ports data
    '''
    df_airport = read_port_data(spark)
    port_df = df_airport \
        .join(immigration_aug_df.select(col('i94port').alias('local_code')).dropDuplicates(), ['local_code'],
              "fullouter") \
        .select('local_code', 'name', 'municipality', 'iso_country', 'elevation_ft'
                , split('coordinates', ',').getItem(0).alias("longitude")
                , split('coordinates', ',').getItem(1).alias("latitude")
                , lit('NA').alias('continent')
                , 'type') \
        .where(df_airport['iso_country'] == "US") \
        .withColumnRenamed('local_code', 'port') \
        .withColumnRenamed('city', 'municipality') \
        .withColumnRenamed('iso_country', 'country') \
        .withColumn('port_id', monotonically_increasing_id())
    return port_df


def process_visa_issuer_data(immigration_aug_df):
    '''
    Process visa issuer data into the correct format.

            Parameters:
                    immigration_aug_df (pyspark.sql.DataFrame): Augmented immigration dataframe

            Returns (pyspark.sql.DataFrame):
                    Visa issuer data
    '''
    visa_issuer_df = immigration_aug_df.select(
        immigration_aug_df.visapost.alias('name'),
    ).dropDuplicates()
    df = visa_issuer_df.withColumn("visa_issuer_id", monotonically_increasing_id())
    return df


def process_alien_data(immigration_aug_df):
    '''
    Process alien data into the correct format.

            Parameters:
                    immigration_aug_df (pyspark.sql.DataFrame): Augmented immigration dataframe

            Returns (pyspark.sql.DataFrame):
                    Alien data
    '''
    alien_df = immigration_aug_df.select(
        to_int(immigration_aug_df.admnum).alias('admission_id'),
        to_country(immigration_aug_df.i94cit).alias('citizenship_origin'),
        to_country(immigration_aug_df.i94res).alias('residency_origin'),
        immigration_aug_df.gender,
        to_int(immigration_aug_df.i94bir).alias('age'),
        immigration_aug_df.visatype,
        to_visa_type(immigration_aug_df.i94visa).alias('i94visa'),
        immigration_aug_df.entdepa,
        immigration_aug_df.entdepd,
        immigration_aug_df.matflag,
        to_int(immigration_aug_df.biryear).alias('year_of_birth'),
    ).dropDuplicates(['admission_id']) \
        .na.drop(subset=["admission_id"])
    return alien_df


def process_logistics_data(immigration_aug_df):
    '''
    Process logistic data into the correct format.

            Parameters:
                    immigration_aug_df (pyspark.sql.DataFrame): Augmented immigration dataframe

            Returns (pyspark.sql.DataFrame):
                    Logistic data
    '''
    logistic_df = immigration_aug_df.select(
        immigration_aug_df.transport_no,
        immigration_aug_df.airline.alias('company'),
        immigration_aug_df.i94mode.alias('mode'),

    ) \
        .dropDuplicates(['transport_no']) \
        .withColumn("transport_id", monotonically_increasing_id())
    return logistic_df


def process_immigrations_fact_data(immigration_aug_df, city_df, port_df, visa_issuer_df, logistic_df):
    '''
    Process immigrations fact data into the correct format.

            Parameters:
                    immigration_aug_df (pyspark.sql.DataFrame): Augmented immigration table
                    city_df (pyspark.sql.DataFrame): City dataframe
                    port_df (pyspark.sql.DataFrame): Port dataframe
                    visa_issuer_df (pyspark.sql.DataFrame): Visa issuer dataframe
                    logistic_df (pyspark.sql.DataFrame): Logistic dataframe

            Returns (pyspark.sql.DataFrame):
                    Immigrations fact data
    '''
    immigration_fact_df = immigration_aug_df \
        .join(city_df,
              ((immigration_aug_df.city == city_df.name) & (immigration_aug_df.state == city_df.state_code)),
              "left") \
        .join(port_df, (immigration_aug_df.i94port == port_df.port), "left") \
        .join(visa_issuer_df, (immigration_aug_df.visapost == visa_issuer_df.name), "left") \
        .join(logistic_df, (immigration_aug_df.transport_no == logistic_df.transport_no), "left") \
        .select(
            col('cic_id'),
            col('city_id').alias('arrival_city'),
            to_timestamp(col('dtaddto')).alias('expiration_time'),
            to_int(col('admnum')).alias('admission_id'),
            to_timestamp(col('sas_arrdate')).alias('arrdate'),
            to_timestamp(col('sas_depdate')).alias('depdate'),
            col('transport_id'),
            col('visa_issuer_id'),
            col('i94Yr').alias('i94_year'),
            col('i94Mon').alias('i94_month')
        )
    return immigration_fact_df


def quality_checks(immigration_fact_df, city_df, port_df, visa_issuer_df, logistic_df, alien_df, time_df):
    '''
    Runs quality check on dataframes

            Parameters:
                    immigration_fact_df (pyspark.sql.DataFrame): Immigration Fact Dataframe
                    city_df (pyspark.sql.DataFrame): City dataframe
                    port_df (pyspark.sql.DataFrame): Port dataframe
                    visa_issuer_df (pyspark.sql.DataFrame): Visa issuer dataframe
                    logistic_df (pyspark.sql.DataFrame): Logistic dataframe
                    alien_df (pyspark.sql.DataFrame): Alien dataframe
                    time_df (pyspark.sql.DataFrame): Time dataframe

            Returns (pyspark.sql.DataFrame):
                    Immigrations fact data
    '''
    def raise_if_null(df, col_name):
        if df.filter(col(col_name).isNull()).count() > 0:
            raise ValueError(f"The column {col_name} in df {df} had a NULL value!")

    raise_if_null(immigration_fact_df, 'cic_id')
    raise_if_null(logistic_df, 'transport_id')
    raise_if_null(visa_issuer_df, 'visa_issuer_id')
    raise_if_null(time_df, 'time')
    raise_if_null(alien_df, 'admission_id')
    raise_if_null(city_df, 'city_id')
    raise_if_null(port_df, 'port_id')


def save_analytics(immigration_fact_df, city_df, port_df, visa_issuer_df, logistic_df, alien_df, time_df):
    '''
    Save data to parquet files

            Parameters:
                    immigration_fact_df (pyspark.sql.DataFrame): Immigration Fact Dataframe
                    city_df (pyspark.sql.DataFrame): City dataframe
                    port_df (pyspark.sql.DataFrame): Port dataframe
                    visa_issuer_df (pyspark.sql.DataFrame): Visa issuer dataframe
                    logistic_df (pyspark.sql.DataFrame): Logistic dataframe
                    alien_df (pyspark.sql.DataFrame): Alien dataframe
                    time_df (pyspark.sql.DataFrame): Time dataframe
    '''
    save_parquet(immigration_fact_df, os.path.join(config.OUTPUT_DATA, 'immigrations', 'immigrations.parquet'), ['i94_year', 'i94_month'])
    save_parquet(city_df, os.path.join(config.OUTPUT_DATA, 'city', 'city.parquet'), [])
    save_parquet(port_df, os.path.join(config.OUTPUT_DATA, 'port', 'visa_issuer.parquet'), [])
    save_parquet(visa_issuer_df, os.path.join(config.OUTPUT_DATA, 'visa_issuer', 'visa_issuer.parquet'), [])
    save_parquet(logistic_df, os.path.join(config.OUTPUT_DATA, 'logistics', 'logistics.parquet'), [])
    save_parquet(alien_df, os.path.join(config.OUTPUT_DATA, 'alien', 'alien.parquet'), [])
    save_parquet(time_df, os.path.join(config.OUTPUT_DATA, 'time', 'time.parquet'), [])


def main():
    spark = create_spark_session()
    df_immigration = read_immigration_data_april(spark) if cloud_config['OTHER']['USE_SAMPLE'] == 'true' else read_immigration_data(spark)
    immigration_aug_df = pre_augment_immigration_data(df_immigration)

    # Dimension tables
    print('Processing dimension tables')
    time_df = process_time_data(immigration_aug_df)
    port_df = process_port_data(spark, immigration_aug_df)
    visa_issuer_df = process_visa_issuer_data(immigration_aug_df)
    alien_df = process_alien_data(immigration_aug_df)
    logistics_df = process_logistics_data(immigration_aug_df)
    city_df = process_city_data(spark, immigration_aug_df)

    # Fact table
    print('Processing fact table')
    immigration_fact_df = process_immigrations_fact_data(immigration_aug_df, city_df, port_df,
                                                         visa_issuer_df, logistics_df)

    # Quality checks
    print('Quality Checks')
    quality_checks(immigration_fact_df, city_df, port_df, visa_issuer_df, logistics_df, alien_df, time_df)

    # Save Analytics
    print('Saving Analytics')
    save_analytics(immigration_fact_df, city_df, port_df, visa_issuer_df, logistics_df, alien_df, time_df)
    print("Done!")


if __name__ == '__main__':
    main()
