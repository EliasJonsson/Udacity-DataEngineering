from datetime import datetime, timedelta
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, TimestampType, DateType

from config import LocalConfigurations,RemoteConfigurations
from utils import read_sas_description

config = RemoteConfigurations()
sas_data = read_sas_description(config.SAS_DESC_DATA_PATH)
sas_ref_date = datetime(1960, 1, 1)

@udf
def to_country(_id):
    try:
        return sas_data['I94CIT'][int(_id)]
    except:
        return "No Country Code"


@udf
def to_port(port_id):
    try:
        return sas_data['I94PORT'][port_id]
    except:
        return "No PORT Code"


@udf
def to_travel_mode(mode_id):
    try:
        return sas_data['I94MODE'][int(mode_id)]
    except:
        return None

@udf
def to_visa_type(visa_id):
    try:
        return sas_data['I94VISA'][visa_id]
    except:
        return None


@udf
def to_transport_no(transport_no, company_name, mode):
    if transport_no:
        return str(transport_no)
    no = 'no'
    if company_name:
        return no
    if company_name:
        no += str(company_name[0:3])
    if mode:
        no += str(mode)
    return no


@udf
def to_int(number):
    if number:
        return int(number)
    return None


@udf(TimestampType())
def to_timestamp_sas(days):
    try:
        return sas_ref_date + timedelta(days=int(days))
    except:
        return None


@udf(DateType())
def to_date_sas(days):
    try:
        return sas_ref_date + timedelta(days=int(days))
    except:
        return None


@udf(StringType())
def upper(val):
    return val.upper()


@udf(StringType())
def city_sas(val):
    try:
        return sas_data['I94PORT'][val][0]
    except:
        return "No PORT Code"


@udf(StringType())
def state_sas(val):
    try:
        return sas_data['I94PORT'][val][1]
    except:
        return ""
