from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):

    ui_color = '#2c3e50'

    def __init__(self,
                 tables,
                 columns,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.columns = columns

    def execute(self, context):
        self.log.info('Connecting to redshift!')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")

            for col in self.columns[table]:
                records = redshift.get_records(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL")
                num_records = records[0][0]
                if num_records > 0:
                    raise ValueError(f"The column {col} in table {table} had a NULL value!")

            self.log.info(f"Data quality on table {table} check passed with {num_records} records")