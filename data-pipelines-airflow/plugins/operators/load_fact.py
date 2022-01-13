from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class LoadFactOperator(BaseOperator):

    ui_color = '#3498db'

    def __init__(self,
                 table,
                 select_sql,
                 redshift_conn_id='redshift',
                 truncate_table = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql
        self.table = table
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info('Connecting to redshift!')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f"Truncating table: {self.table}")
            redshift.run(f"""
                        TRUNCATE TABLE {self.table};
                    """)

        self.log.info(f"Loading fact table `{self.table}` into redshift")
        redshift.run(f"""
            INSERT INTO {self.table}
            {self.select_sql};
        """)
