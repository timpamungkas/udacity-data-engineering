from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Load staging table into fact table
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info('LoadFactOperator starting')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        table_insert_sql = "INSERT INTO {} {}".format(self.table, self.select_sql)
        
        redshift_hook.run(table_insert_sql)