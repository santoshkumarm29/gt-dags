s3_import_params = {
    "aws_conn_id": "ww_s3_import",
    "vertica_conn_id": "vertica_conn",
    "resourcepool": "default",
    "s3_bucket_name": "worldwinner-data-imports-staging",
    "look_back_hours": 3,
    "number_of_tasks": 10,
    "task_prefix": "t_ww_task_stg_",
    "env": "STG",
    "slack_secret_key": "slack_api_key",
    "send_slack_notification_YN": "Y"
}
