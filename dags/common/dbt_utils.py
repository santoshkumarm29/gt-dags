"""
Helper functions for dbt management.
"""
import boto3
import os
from airflow.models import Variable
import os
import shutil
import subprocess

_dbt_bin_location = "/usr/local/airflow/.local/bin/dbt"

def __upload_files(local_files_paths: list, bucket_name: str, bucket_folder: str):
    s3 = boto3.resource('s3')
    for file_path in local_files_paths:
      target_s3_path = bucket_folder+"/"+os.path.basename(file_path)
      s3.Object(bucket_name, target_s3_path).upload_file(file_path)
      print("uploaded " + file_path + " to "+ target_s3_path)

def __get_dbt_command():
  return [_dbt_bin_location,'--no-write-json','-d']

def __get_vertica_connection_settings():
  vertica_user = Variable.get("vertica_user")
  vertica_password = Variable.get("vertica_password")
  vertica_db_name = Variable.get("vertica_db_name")
  vertica_host = Variable.get("vertica_host")

  return {
  "vertica_user": vertica_user,
  "vertica_password": vertica_password,
  "vertica_host": vertica_host,
  "vertica_db_name": vertica_db_name
}

def __copy_dbt(current_dbt_path: str, target_dbt_path:str):
  print("copying "+ current_dbt_path + " to "+target_dbt_path)
  shutil.rmtree(target_dbt_path,ignore_errors = True)
  shutil.copytree(current_dbt_path, target_dbt_path)

def __run_os_command(command,env,dbt_path):
  try:
    res = subprocess.Popen(command, 
                           stdout=subprocess.PIPE,
                           env=env,
                           cwd=dbt_path,
                           universal_newlines=True)
  except OSError as e:
    print(e)
    raise e

  res.wait()
  if res.returncode != 0:
    raise ValueError('The command was not succesfull')

  result = res.stdout.read()
  [print(i) for i in result.splitlines()]
  

def run_dbt(parent_dag_name:str,scripts_bucket_name:str,docs_s3_path:str,dbt_path:str):
  temp_dbt_path = os.path.join("/tmp",parent_dag_name)
  dbt_command = __get_dbt_command()

  #setting env variables
  vertica_connection = __get_vertica_connection_settings()

  #copy dbt
  __copy_dbt(current_dbt_path = dbt_path, target_dbt_path=temp_dbt_path)
  #run dbt
  __run_os_command(dbt_command+['run', '--profiles-dir', 'profiles/vertica'],vertica_connection,temp_dbt_path)
  #create docs
  __run_os_command(dbt_command + ['docs', 'generate', '--profiles-dir','profiles/vertica'],vertica_connection,temp_dbt_path)
  # ls the target folder
  __run_os_command(['ls', '-R', temp_dbt_path],vertica_connection,temp_dbt_path)
  # upload the docs to s3
  __upload_files(local_files_paths= [temp_dbt_path+'/target/index.html',temp_dbt_path+'/target/catalog.json'], bucket_name= scripts_bucket_name, bucket_folder= docs_s3_path)