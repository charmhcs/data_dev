from typing import TYPE_CHECKING, Dict, Iterable, List, Mapping, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.oracle.hooks.oracle import OracleHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OracleQueryOperator(BaseOperator):

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: Union[str, List[str]],
        oracle_conn_id: str = 'oracle_default',
        parameters: Optional[Union[Mapping, Iterable]] = None,
        autocommit: bool = False,
        return_type=None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.return_type = return_type

    def execute(self, context: 'Context') -> None:
        self.log.info('Executing: %s', self.sql)

        # 바인드변수 처리
        if self.parameters:
            bind_names = ','.join([f':{i+1}' for i in range(len(self.parameters))])
            self.sql = self.sql.replace(':bind_names', bind_names)

        """쿼리 실행하고 결과 반환"""
        hook = OracleHook(oracle_conn_id=self.oracle_conn_id,
                          thick_mode=True)
        if self.sql:
            if self.return_type == 'df_json':
                result = hook.get_pandas_df(self.sql, parameters=self.parameters)
                self.log.info(result.info())
                return result.to_json(orient='columns')
            else:
                result = hook.get_records(self.sql, parameters=self.parameters)
                self.log.info(result)
                return result

