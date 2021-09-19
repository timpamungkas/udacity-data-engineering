from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Runs data quality check by validate result vs test SQL query (both passed as parameters)
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 validate_query="",
                 expected_result="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.validate_query = validate_query
        self.expected_result = expected_result

    def execute(self, context):
        self.log.info('DataQualityOperator starting')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift_hook.get_records(self.validate_query)
        if records[0][0] != self.expected_result:
            raise ValueError("Failed : {} does not match expected {}".format(records[0][0], self.expected_result))