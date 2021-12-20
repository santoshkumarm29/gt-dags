# README #

## What is this repository for?

Contains all the dags and dependencies that are deployed to the AWS managed airflow

# How do I add a new dag?

Look at the many examples in the dags folder and create your new DAG in one of the following structures:

**Simple dag**
```
dags
└──name_of_your_dag
    └── name_of_your_dag.py
```

**Dag that uses sensors**
```
dags
└──name_of_your_dag
    ├── name_of_your_dag.py
    └── sql
        └── sensor_query.sql
```

**Dag with sensors and DBT***
```
dags
└──name_of_your_dag
    ├── dbt
    │   ├── dbt_project.yml
    │   ├── models
    │   │   ├── model_1.sql
    │   │   ├── model_2.sql
    │   │   └── model_3.sql
    │   └── profiles
    │       └── vertica
    │           └── profiles.yml
    ├── name_of_your_dag.py
    └── sql
        └── sensor_query.sql
```

## Notes
- The name of the py file that contains your dag must end in `_dag`

# DAGs cleanup

## Overall steps

1. Create a new branch on this repo (one branch for each dag)
2. Get the DAG you have assigned and move it with it dependencies to an structure matching one of the structures shown in `How do I add a new dag?`
3. Fix any importing errors in the python files, take a look at `etl-dags/dags/phoenix_frequent_dag` and `etl-dags/dags/fraud_mailer_ww` to fix the imports
4. Apply DBT if needed (see the `DBT` section below)
5. Clean the SQL statements if needed (see the `SQL cleanup` section below)
6. Push your branch and notify Juan Luna or Vianel Rodriguez via teams about the push.

## DBT

You need to use DBT if in step 2 you find one of this:
- there are huge sql files with more than one statement
- there are statements that can be split
- there are tasks that execute sql statements that are chained (task1 creates table_a, task2 uses table_a...)

Check the `etl-dags/dags/phoenix_frequent_dag` to see some dbt examples. 

### Steps
1. Copy the DBT folder and work your sql statements in the models folder.
2. Update the dag python script to have the dbt tasks, use `etl-dags/dags/phoenix_frequent_dag/phoenix_frequent_dag.py` as a reference

### Notes
- The profiles.yml file should remain the same except for the schema
- Take a look at the [DBT model docs](https://docs.getdbt.com/docs/building-a-dbt-project/building-models)
- If you are going to use an incremental materialization, but it doesn't have a unique key, add the `incremental_strategy='delete+insert'` config, one example of it can be found in  `etl-dags/dags/phoenix_frequent_dag/dbt/models/t_update_dim_device_mapping_raw.sql`
- In the first test in MWAA the subdag won't run, it will appear as running from the main dag, but zooming in the subdag you will notice that the task haven't run; go to the `Code` tab in Airflow UI and enable the subdag.

## SQL cleanup

Beyond the dag cleanup, you will also need to clean the SQL statements by removing references to non Gametaco applications

**Gametaco applications**
- Phoenix
- Anything that contains WW in it
- Anything related to worldwinner

**Applications to remove**
- WOF
- Grand Casino and similars
- Bash
- BingoBash
- Fresh deck
- Slots bash
- Tripeaks
- Canvas