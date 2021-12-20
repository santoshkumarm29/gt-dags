## General issues
- ssl not supprted by vertica server
-missing SES (TODO)

## DAG TEST 2021-11-22

- ENV = accelerator prod

### etl_gsn_daily_kpi_email_flash_ww_only_test_000

Pandas error

- missing kpi.daily_email_output_latest_v

### etl_gsn_daily_kpi_email_ww_only_test_000

OK

### etl_phoenix_activity

OK

### etl_phoenix_frequent_jobs

OK

### etl_phoenix_sessions

OK

### etl_phoenix_tournaments 

OK

### etl_ww_events_web_sessions

OK

### etl_ww_user_day

OK

### fraud_mailer_ww_001

OK

### kochava_device_summary_backfill_000

OK

### ~~kochava_importer_test_ua_main_000~~

SKIPPED

### kochava_importer_ua_main_003

```
Requesting install data on account gamesnetwork for app PHOENIX, android: kogsn-phoenix-android-vjuqa from 2021-11-20 20:30:00 to 2021-11-21 20:30:00.
post status 200 : https://reporting.api.kochava.com/v1.4/detail : {"api_key": "123456\n", "app_guid": "kogsn-phoenix-android-vjuqa", "time_start": "1637469000", "time_end": "1637555400", "traffic": ["install"], "delivery_format": "json", "delivery_method": ["S3link"], "notify": ["dummy@gsngames.com"], "time_zone": "America/Los_Angeles", "traffic_including": ["unattributed_traffic"]}
Error Occured when trying to post data for account : gamesnetwork with error b'{"status":"Error","error":"Error request parameters: Error. Invalid reporting API Key"}\n' check the parameters
```
- missing `gsnmobile.ad_partner_installs_gamesnetwork_ods`
- missing `gsnmobile.ad_partner_installs_staging`

### skill_partner_reports_001

- missing email config

### ua_master_main_dag_001

OK

### ua_master_main_dag.kochava_click_importer_subdag_001

```
Requesting click data on account gamesnetwork for app PHOENIX, ios: kogsn-phoenix-ios-xkv from 2021-11-20T03:00:00-08:00 to 2021-11-20T17:00:00-08:00.
post status 200 : https://reporting.api.kochava.com/v1.4/detail : {"api_key": "123456\n", "app_guid": "kogsn-phoenix-ios-xkv", "time_start": "1637406000", "time_end": "1637456400", "traffic": ["click"], "delivery_format": "json", "delivery_method": ["S3link"], "notify": ["dummy@gsngames.com"], "time_zone": "America/Los_Angeles"}
Error Occured when trying to post data for account : gamesnetwork with error b'{"status":"Error","error":"Error request parameters: Error. Invalid reporting API Key"}\n' check the parameters
Requesting click data on account gamesnetwork for app WW SOLRUSH, ios: koworldwinner-mobile-solitaire-j4e from 2021-11-20T03:00:00-08:00 to 2021-11-20T17:00:00-08:00.
post status 200 : https://reporting.api.kochava.com/v1.4/detail : {"api_key": "123456\n", "app_guid": "koworldwinner-mobile-solitaire-j4e", "time_start": "1637406000", "time_end": "1637456400", "traffic": ["click"], "delivery_format": "json", "delivery_method": ["S3link"], "notify": ["dummy@gsngames.com"], "time_zone": "America/Los_Angeles"}
Error Occured when trying to post data for account : gamesnetwork with error b'{"status":"Error","error":"Error request parameters: Error. Invalid reporting API Key"}\n' check the parameters
```

- missing `gsnmobile.ad_partner_clicks_ods`

### ua_master_main_dag.kochava_impressions_importer_subdag_000

Missing gsnmobile.ad_partner_impressions_ods
Missing gsnmobile.ad_partner_impressions

### ua_master_main_dag.singular_subdag_000

FAILED: Missing singular_api_key

### ~~ua_master_main_dag.singular_v2_dag_000~~

Skipped

### ~~ua_master_main_dag.singular_v3_dag_0000~~

Skipped

### ua_master_skill_dag_000

- Could not convert "81636867-77e6-43a1-9565-c33e4f3f1a2a" from column e.id to a float8

### worldwinner_ftd_ltr_prediction_v1.0

FAILED: missing model files, data test can not be completed.

### worldwinner_ltc_prediction_v1_0

FAILED: missing model files, data test can not be completed.

### worldwinner_reg_ltr_prediction_v1.0

FAILED: "None of ['user_id'] are in the columns"

### worldwinner_responsys_daily_imports

FAILED: RESPONSYS_SSH_KEY MISSING. No other missing objects.

### worldwinner_segments_prediction_v1.3

FAILED: missing model files, data test can not be completed.

### ww_aggs_000

OK

### ww_app_data_import_hourly_dag_000

OK

### ~~ww_app_data_import_hourly_dag_qa~~

SKIPPED

### ~~ww_app_data_import_hourly_dag_stg~~

SKIPPED

### ~~ww_app_data_import_hourly_dag_v2~~

SKIPPED

### ww_ltv_calculations_000

OK

### ww_mobile_fixed_observation_features_v2.1

OK (very slow)

### ww_pii_gdpr

OK

### ww_responsys_import_001

FAILED: missing sftp credentials, data test can not be completed, but no object is missing form the database

