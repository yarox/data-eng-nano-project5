from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator


class DataQualityOperator(BaseOperator):
    '''
    Execute quality checks by accepting one or more SQL based test cases along
    with the expected results.

    Args:
        conn_id: Connection ID for Redshift.
        cases: List of {query, expected_result} dicts, where "query" is a valid
            SQL query that returns a number and "expected_result" is a number.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.

    Raises:
        ValueError: If the actual result and the expected result don't match.
    '''

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self, conn_id, cases=None, *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.cases = cases or []

    def execute(self, context):
        postgres_hook = PostgresHook(self.conn_id)

        self.log.info(f'Running {len(self.cases)} quality checks.')

        for i, case in enumerate(self.cases):
            expected_result = case['expected_result']
            query = case['query']

            output = postgres_hook.get_records(query)
            actual_result = output[0][0]

            if expected_result == actual_result:
                self.log.info(f'{i} - Data quality check for "{query}" passed.')

            else:
                message = (
                    f'{i} - Data quality check for "{query}" failed. '
                    f'Expected "{expected_result}" records '
                    f'but got "{actual_result}" records.'
                )

                self.log.info(message)
                raise ValueError(message)
