-- clid_combos_ext
DELETE /*+ DIRECT,LABEL('airflow-{{ dag.safe_dag_id }}-{{ task.task_id }}') */
FROM ww.clid_combos_ext
WHERE client_id IN
( SELECT client_id
  FROM ww.clid_combos c , ww.user_ids_to_purge u
  WHERE c.user_id = u.user_id);

-- clid_combos
DELETE /*+ DIRECT,LABEL('airflow-{{ dag.safe_dag_id }}-{{ task.task_id }}') */
FROM ww.clid_combos
WHERE user_id IN (SELECT user_id FROM ww.user_ids_to_purge u );

-- ww.user_opt_ins
DELETE /*+ DIRECT,LABEL('airflow-{{ dag.safe_dag_id }}-{{ task.task_id }}') */
FROM ww.user_opt_ins
WHERE user_id IN (SELECT user_id FROM ww.user_ids_to_purge u );

-- ww.dim_email
DELETE /*+ DIRECT,LABEL('airflow-{{ dag.safe_dag_id }}-{{ task.task_id }}') */
FROM ww.dim_email
WHERE user_id IN (SELECT user_id FROM ww.user_ids_to_purge u );

-- ww.user_rank_match
DELETE /*+ DIRECT,LABEL('airflow-{{ dag.safe_dag_id }}-{{ task.task_id }}') */
FROM ww.user_rank_match
WHERE user_id IN (SELECT user_id FROM ww.user_ids_to_purge u );

-- ww.played_game_platform
DELETE /*+ DIRECT,LABEL('airflow-{{ dag.safe_dag_id }}-{{ task.task_id }}') */
FROM ww.played_game_platform
WHERE user_id IN (SELECT user_id FROM ww.user_ids_to_purge u );

commit;
