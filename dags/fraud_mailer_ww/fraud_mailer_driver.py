"""
Driver for fraud emails.
"""
import logging
import pathlib
import os

from common.email_utils import send_email
from common.hooks.vertica_hook import VerticaHook

logger = logging.getLogger('fraud_mailer_ww_001')

def mass_mail(email_sql_path, body_format, to_address, from_address, subject, **kwargs):
    """
    Send fraud a email per record per query.
    """
    my_path = pathlib.Path(__file__).parent.absolute()
    sql_path = os.path.join(my_path, 'sql')
    logger.info('mass_mail <%s:%s:%s>', to_address, subject, email_sql_path)
    label = 'airflow-skill-' + kwargs['dag'].dag_id + '-' + kwargs['task'].task_id
    today = kwargs['tomorrow_ds']
    with open(os.path.join(sql_path, email_sql_path), 'rt') as file:
      email_sql = file.read()
    email_sql = email_sql.format(label=label, today=today)
    conn = VerticaHook(
        vertica_conn_id='vertica_conn',
        resourcepool='default').get_conn()
    with conn.cursor() as curs:
        curs.execute(email_sql)
        rows = curs.fetchall()
    for row in rows:
        subj = subject.format(*row)
        msg = body_format.replace('\n', '<br/>').format(*row)
        send_email(source=from_address,
                   to_addresses=to_address,
                   subject=subj,
                   message=msg)


takeover_format = """
user_id: {}
username: {}
create_date: {}
password: {}
email: {}
thirty day avg: {}
yday: {}
% increase: {}
diff: {}
real money date: {}
advertiser_id: {}
"""

progressive_format = """
Tournament Id: {}
Template Id: {}
Tournament Name: {}
Registration Open: {}
Registration Close: {}
Prize Percent: {}
Type Name: {}
Prize: {}
"""

fpue_format = """
Tournament Id: {}
Template Id: {}
Tournament Name: {}
Registration Open: {}
Registration Close: {}
Fee: {}
Purse: {}
Type: {}
"""

ww_new_players_format = """
Create Date: {}
User Id: {}
Username: {}
Advertiser Id: {}
Marketing Parnertag: {}
Deposits: {}
Deposit Amount: {}
User Banned Date: {}
Email: {}
Real Money Date: {}
"""