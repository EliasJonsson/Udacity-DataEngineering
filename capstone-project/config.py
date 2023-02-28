import pathlib
import os

import configparser
FILE_PATH = pathlib.Path(__file__).parent.resolve()

cloud_config = configparser.ConfigParser()
cloud_config.read(os.path.join(FILE_PATH, 'cloud.cfg'))
is_aws_access_keys_set = 'AWS_ACCESS_KEY_ID' in os.environ or 'AWS_SECRET_ACCESS_KEY' in os.environ
is_aws_profile_set = 'AWS_PROFILE' in os.environ
if (not is_aws_access_keys_set and not is_aws_profile_set):
    os.environ['AWS_ACCESS_KEY_ID'] = cloud_config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = cloud_config['AWS']['AWS_SECRET_ACCESS_KEY']




class LocalConfigurations:
	def __init__(self):
		self.TEMPERATURE_DATA_PATH = os.path.join(FILE_PATH, "data", "GlobalLandTemperaturesByCity.csv")
		self.DEMOGRAPHICS_DATA_PATH = os.path.join(FILE_PATH, "data", "us-cities-demographics.csv")
		self.AIRPORT_DATA_PATH = os.path.join(FILE_PATH, "data", "airport-codes_csv.csv")
		self.SAS_DESC_DATA_PATH = os.path.join(FILE_PATH, "I94_SAS_Labels_Descriptions.SAS")
		self.SAS_DATA_PATH = os.path.join(FILE_PATH, "data", "i94", "18-83510-I94-Data-2016")
		self.SAS_DATA_SAMPLE_PATH = os.path.join(FILE_PATH, "data", "immigration_data_sample.csv")
		self.OUTPUT_DATA = os.path.join(FILE_PATH, "artifacts", "analytics")


class RemoteConfigurations:
	def __init__(self):
		self.TEMPERATURE_DATA_PATH = os.path.join(cloud_config['S3']['INPUT_FOLDER'], "GlobalLandTemperaturesByCity.csv")
		self.DEMOGRAPHICS_DATA_PATH = os.path.join(cloud_config['S3']['INPUT_FOLDER'], "us-cities-demographics.csv")
		self.AIRPORT_DATA_PATH = os.path.join(cloud_config['S3']['INPUT_FOLDER'], "airport-codes_csv.csv")
		self.SAS_DESC_DATA_PATH = os.path.join(FILE_PATH, "I94_SAS_Labels_Descriptions.SAS")
		self.SAS_DATA_PATH = os.path.join(cloud_config['S3']['INPUT_FOLDER'], "i94", "18-83510-I94-Data-2016")
		self.SAS_DATA_SAMPLE_PATH = os.path.join(cloud_config['S3']['INPUT_FOLDER'], "immigration_data_sample.csv")
		self.OUTPUT_DATA = os.path.join(cloud_config['S3']['OUTPUT_FOLDER'], "analytics-final")