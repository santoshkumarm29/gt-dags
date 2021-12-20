DROP TABLE ww.responsys_bounce;
DROP TABLE ww.responsys_bounce_ods;

CREATE TABLE IF NOT EXISTS ww.responsys_bounce(
  event_type_id INTEGER,
  account_id INTEGER,
  list_id INTEGER,
  riid INTEGER,
  customer_id VARCHAR(100),
  event_captured_dt timestamp,
  event_stored_dt timestamp,
  campaign_id integer,
  launch_id integer,
  email varchar(500),
  email_format varchar(36),
  bounce_type varchar(36),
  reason varchar(500),
  reason_code varchar(500),
  subject varchar(500),
  contact_info varchar(500),
  file_name varchar(100)
)
;

CREATE TABLE IF NOT EXISTS ww.responsys_bounce_ods LIKE ww.responsys_bounce;

GRANT ALL ON ww.responsys_bounce TO airflow_etl_role;
GRANT ALL ON ww.responsys_bounce_ods TO airflow_etl_role;

DROP TABLE ww.responsys_click;
DROP TABLE ww.responsys_click_ods;

CREATE TABLE ww.responsys_click(
  event_type_id INTEGER,
  account_id integer,
  list_id integer,
  riid integer,
  customer_id VARCHAR(100),
  event_captured_dt timestamp,
  event_stored_dt timestamp,
  campaign_id integer,
  launch_id integer,
  email_format varchar(36),
  offer_name varchar(100),
  offer_number integer,
  offer_category varchar(36),
  offer_url varchar(500),
  file_name varchar(100)
)
;

CREATE TABLE IF NOT EXISTS ww.responsys_click_ods LIKE ww.responsys_click;

GRANT ALL ON ww.responsys_click TO airflow_etl_role;
GRANT ALL ON ww.responsys_click_ods TO airflow_etl_role;

DROP TABLE ww.responsys_launch_state;
DROP TABLE ww.responsys_launch_state_ods;

CREATE TABLE ww.responsys_launch_state(
  account_id integer,
  list_id integer,
  event_captured_dt timestamp,
  event_stored_dt timestamp,
  campaign_id integer,
  launch_id integer,
  external_campaign_id integer,
  sf_campaign_id integer,
  campaign_name varchar(100),
  launch_name varchar(100),
  launch_status varchar(100),
  launch_type varchar(100),
  launch_charset varchar(100),
  purpose varchar(500),
  subject varchar(500),
  description varchar(500),
  product_category varchar(36),
  product_type varchar(36),
  marketing_strategy varchar(100),
  marketing_program varchar(100),
  launch_error_code varchar(36),
  launch_started_dt timestamp,
  launch_completed_dt timestamp,
  file_name varchar(100)
);

CREATE TABLE IF NOT EXISTS ww.responsys_launch_state_ods LIKE ww.responsys_launch_state;

GRANT ALL ON ww.responsys_launch_state TO airflow_etl_role;
GRANT ALL ON ww.responsys_launch_state_ods TO airflow_etl_role;


DROP TABLE ww.responsys_open;
DROP TABLE ww.responsys_open_ods;

CREATE TABLE ww.responsys_open(
  event_type_id INTEGER,
  account_id INTEGER,
  list_id INTEGER,
  riid INTEGER,
  customer_id VARCHAR(100),
  event_captured_dt timestamp,
  event_stored_dt timestamp,
  campaign_id integer,
  launch_id integer,
  email_format varchar(100),
  file_name varchar(100)
);

CREATE TABLE IF NOT EXISTS ww.responsys_open_ods LIKE ww.responsys_open;

GRANT ALL ON ww.responsys_open TO airflow_etl_role;
GRANT ALL ON ww.responsys_open_ods TO airflow_etl_role;

DROP TABLE ww.responsys_program;
DROP TABLE ww.responsys_program_ods;

CREATE TABLE ww.responsys_program(
  event_type_id INTEGER,
  account_id INTEGER,
  list_id INTEGER,
  riid INTEGER,
  customer_id VARCHAR(100),
  event_captured_dt timestamp,
  event_stored_dt timestamp,
  program_id integer,
  enactment_id integer,
  program_name varchar(100),
  program_version varchar(100),
  description varchar(100),
  file_name varchar(100)
);

CREATE TABLE IF NOT EXISTS ww.responsys_program_ods LIKE ww.responsys_program;

GRANT ALL ON ww.responsys_program TO airflow_etl_role;
GRANT ALL ON ww.responsys_program_ods TO airflow_etl_role;

DROP TABLE ww.responsys_sent;
DROP TABLE ww.responsys_sent_ods;

CREATE TABLE ww.responsys_sent(
  event_type_id INTEGER,
  account_id INTEGER,
  list_id INTEGER,
  riid INTEGER,
  customer_id VARCHAR(100),
  event_captured_dt timestamp,
  event_stored_dt timestamp,
  campaign_id integer,
  launch_id integer,
  email varchar(500),
  email_isp varchar(100),
  email_format varchar(100),
  offer_signature_id integer,
  dynamic_content_signature_id integer,
  message_size integer,
  segment_info varchar(100),
  contact_info varchar(100),
  file_name varchar(100)
);


CREATE TABLE IF NOT EXISTS ww.responsys_sent_ods LIKE ww.responsys_sent;

GRANT ALL ON ww.responsys_sent TO airflow_etl_role;
GRANT ALL ON ww.responsys_sent_ods TO airflow_etl_role;


DROP TABLE ww.responsys_fail;
DROP TABLE ww.responsys_fail_ods;

CREATE TABLE ww.responsys_fail(
    event_type_id INTEGER,
  account_id INTEGER,
  list_id INTEGER,
  riid INTEGER,
  customer_id VARCHAR(100),
  event_captured_dt timestamp,
  event_stored_dt timestamp,
  campaign_id integer,
  launch_id integer,
  email varchar(500),
  email_isp varchar(100),
  email_format varchar(100),
  offer_signature_id integer,
  dynamic_content_signature_id integer,
  message_size integer,
  segment_info varchar(100),
  contact_info varchar(100),
  file_name varchar(100)
);

CREATE TABLE IF NOT EXISTS ww.responsys_fail_ods LIKE ww.responsys_fail;

GRANT ALL ON ww.responsys_fail TO airflow_etl_role;
GRANT ALL ON ww.responsys_fail_ods TO airflow_etl_role;

