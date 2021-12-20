import logging
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from common.hooks.vertica_hook import VerticaHook


class GsnSqlSensor(BaseSensorOperator):
    """
    exact replica of airflow BaseSensorOperator with only difference being
    this class accepts resoursepool as an extra parameter
    Runs a sql statement until a criteria is met.
    default behavior :
        It will keep trying while
        sql returns no row, or if the first cell in (0, '0', '')
    inverse is True :
        It will keep trying while
        sql returns row, or if the first cell is not (0, '0', '')

    :param conn_id: The connection to run the sensor against
    :type conn_id: string
    :param sql: The sql to run. To pass, it needs to return at least one cell
        that contains a non-zero value.
    """
    template_fields = ('sql',)
    template_ext = ('.hql', '.sql',)

    @apply_defaults
    def __init__(self, conn_id, sql, resourcepool=None, timeout=60*60*2, poke_interval=60*10, inverse=False, *args, **kwargs):
        self.conn_id = conn_id
        self.sql = sql
        self.resourcepool = resourcepool
        self.inverse = inverse
        super(GsnSqlSensor, self).__init__(timeout=timeout, poke_interval=poke_interval, *args, **kwargs)

    def poke_logic(self, records):
        if not records:
            '''
                Normal Behavior 
            '''
            if not self.inverse:
                return False
            else:
                '''
                    Opposite to the Normal Behavior 
                '''
                return True
        else:
            '''
                Normal Behavior 
            '''
            if not self.inverse:
                if str(records[0][0]) in ('0', '',):
                    return False
                else:
                    return True
            else:
                '''
                    Opposite to the Normal Behavior 
                '''
                if str(records[0][0]) not in ('0', '',):
                    return False
                else:
                    return True

    def poke(self, context):
        connection = VerticaHook(vertica_conn_id=self.conn_id, resourcepool=self.resourcepool).get_conn()

        logging.info('Poking: ' + self.sql)
        records = self.get_records(self.sql, connection)
        connection.close()
        return self.poke_logic(records)

    @staticmethod
    def get_records(sql, conn):
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchall()
