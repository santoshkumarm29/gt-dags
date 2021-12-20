"""
Driver for fraud emails.
"""
import datetime
import os
import logging

from common.email_utils import send_email
from common.hooks.vertica_hook import VerticaHook

dag_id = 'fraud_mailer_ww_001'
schedule = '30 13 * * *'
logger = logging.getLogger(dag_id)

default_args = {
    'owner': 'skill',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 11, 21),
    'email': ['etl@gsngames.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}


env_name = os.environ.get('ENVIRONMENT_NAME', 'dsdev')
if env_name == 'dsdev':
    test_address = 'jbondurant@gsngames.com'
    fraud_address = [test_address]
    gsnfraud_address = [test_address]
    from_address = test_address
else:
    fraud_address = ['fraud@gsngames.com']
    gsnfraud_address = ['gsnfraud@gsngames.com']
    from_address = 'vertia_fraud_mails@gsngames.com'



def mass_mail(email_sql, body_format, to_address, from_address, subject, **kwargs):
    """
    Send fraud a email per record per query.
    """
    logger.info('mass_mail <%s:%s>', to_address, subject)
    label = 'airflow-skill-' + kwargs['dag'].dag_id + '-' + kwargs['task'].task_id
    today = kwargs['tomorrow_ds']
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




fraud_email_config = [
    {
        'email_name': 'FPUE Data',
        'email_sql': 'fpue.sql',
        'body_format': fpue_format,
        'to_address': fraud_address,
        'from_address': from_address,
        'subject': 'Fraud FPUE Review Alert - {6}',
    },
    {
        'email_name': 'Progressive Data',
        'email_sql': 'progressive.sql',
        'body_format': progressive_format,
        'to_address': fraud_address,
        'from_address': from_address,
        'subject': 'Fraud Progressive Review Alert - {7}',
    },
    {
        'email_name': 'WW New Players',
        'email_sql': 'ww_new_players.sql',
        'body_format': ww_new_players_format,
        'to_address': fraud_address,
        'from_address': from_address,
        'subject': 'Cash Games New User - {2}',
    },
    {
        'email_name': 'WW Takeover',
        'email_sql': 'ww_takeover.sql',
        'body_format': takeover_format,
        'to_address': fraud_address,
        'from_address': from_address,
        'subject': 'Account Takeover, Overspending, Fraud on WW - {1}',
    }
]
