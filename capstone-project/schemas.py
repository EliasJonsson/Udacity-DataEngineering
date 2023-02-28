from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, DateType

demographics_schema = StructType([
    StructField('city', StringType()),
    StructField('state', StringType()),
    StructField('median_age', DoubleType()),
    StructField('male_population', DoubleType()),
    StructField('female_population', DoubleType()),
    StructField('total_population', IntegerType()),
    StructField('number_of_veterans', DoubleType()),
    StructField('foreign_born', DoubleType()),
    StructField('average_household_size', DoubleType()),
    StructField('state_code', StringType()),
    StructField('race', StringType()),
    StructField('count', IntegerType()),
]) 

airport_schema = StructType([
    StructField('ident', StringType()),
    StructField('type', StringType()),
    StructField('name', StringType()),
    StructField('elevation_ft', IntegerType()),
    StructField('continent', StringType()),
    StructField('iso_country', StringType()),
    StructField('iso_region', StringType()),
    StructField('municipality', StringType()),
    StructField('gps_code', StringType()),
    StructField('iata_code', StringType()),
    StructField('local_code', StringType()),
    StructField('coordinates', StringType()),
])

world_temp_schema = StructType([
    StructField('dt', StringType()),
    StructField('average_temperature', DoubleType()),
    StructField('average_temperature_uncertainty', DoubleType()),
    StructField('city', StringType()),
    StructField('country', StringType()),
    StructField('latitude', StringType()),
    StructField('longitude', StringType()),
])


immigrations_schema = StructType([
    StructField('cic_id', DoubleType()),
    StructField('i94yr', DoubleType()),
    StructField('i94mon', DoubleType()),
    StructField('i94cit', DoubleType()),
    StructField('i94res', DoubleType()),
    StructField('i94port', StringType()),
    StructField('sas_arrdate', DoubleType()),
    StructField('i94mode', DoubleType()),
    StructField('i94addr', StringType()),
    StructField('sas_depdate', DoubleType()),
    StructField('i94bir', DoubleType()),
    StructField('i94visa', DoubleType()),
    StructField('count', DoubleType()),
    StructField('dtadfile', StringType()),
    StructField('visapost', StringType()),
    StructField('occup', StringType()),
    StructField('entdepa', StringType()),
    StructField('entdepd', StringType()),
    StructField('entdepu', StringType()),
    StructField('matflag', StringType()),
    StructField('biryear', DoubleType()),
    StructField('dtaddto', StringType()),
    StructField('gender', StringType()),
    StructField('insnum', StringType()),
    StructField('airline', StringType()),
    StructField('admnum', DoubleType()),
    StructField('fltno', StringType()),
    StructField('visatype', StringType()),
])




