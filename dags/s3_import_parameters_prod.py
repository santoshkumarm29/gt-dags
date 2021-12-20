s3_import_params = {
    "aws_conn_id": "ww_s3_import",
    "vertica_conn_id": "vertica_conn",
    "resourcepool": "default",
    "s3_bucket_name": "worldwinner-data-imports",
    "look_back_hours": 5,
    "number_of_tasks": 10,
    "task_prefix": "t_ww_task_prod_",
    "env": "PROD",
    "slack_secret_key": "slack_api_key"
}
