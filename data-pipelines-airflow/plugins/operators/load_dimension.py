from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class LoadDimensionOperator(BaseOperator):

    ui_color = '#e74c3c'

    def __init__(self,
                 table,
                 select_sql,
                 redshift_conn_id='redshift',
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info('Connecting to redshift!')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f"Truncating table: {self.table}")
            redshift.run(f"""
                        TRUNCATE TABLE {self.table};
                    """)

        self.log.info('Loading dimension table into redshift')
        redshift.run(f"""
            INSERT INTO {self.table}
            {self.select_sql}
        """)
