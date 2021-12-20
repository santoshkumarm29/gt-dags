DELETE /*+direct, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}')*/
FROM ww.agg_daily_margin
WHERE "date" >= '{{ ds }}';

INSERT /*+direct, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}')*/
    INTO ww.agg_daily_margin (date, gametype_ID, margin)
        SELECT DISTINCT date(trans_date) AS date,
	       tg.gametype_id,
	       ((sum(CASE WHEN trans_type = 'SIGNUP' THEN amount * - 1 ELSE 0 END)) - (sum(CASE WHEN trans_type = 'WINNINGS' THEN amount ELSE 0 END))) / sum(nullif(CASE WHEN trans_type = 'SIGNUP' THEN amount * - 1 ELSE 0 END,0)) as margin
		FROM ww.internal_transactions it
		INNER JOIN ww.tournament_games tg
			on it.product_id = tg.tournament_id
		WHERE trans_type in ('SIGNUP','WINNINGS')
		AND trans_date >= '{{ ds }}'
		GROUP BY 1,2;

DELETE /*+direct, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}')*/
FROM ww.agg_cash_game_stats_daily
WHERE "date" >= '{{ ds }}';

INSERT /*+direct, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}')*/
    INTO ww.agg_cash_game_stats_daily (
        date,
        gametype_ID,
        game_name,
        platform,
        margin,
        cef,
        tef,
        winnings,
        nar,
        new_players,
        legacy_players,
        total_players,
        new_player_signups,
        legacy_player_signups,
        total_signups,
        nar_actual,
        tournaments)

SELECT distinct date(t.trans_date) as date
	, tg.gametype_ID
	, g.name as game_name
	, case
		when c.device is null then 'DESKTOP'
		when c.device is not null and (c.platform not in ('skill-app','skill-solrush') or c.platform is null) then 'MOWEB'
		when c.device is not null and c.platform = 'skill-app' and c.operating_system ilike '%ios%' then 'MOAPP - iPhone'
		when c.device is not null and c.platform = 'skill-app' and c.operating_system ilike '%android%' then 'MOAPP - Android'
		when c.device is not null and c.platform = 'skill-solrush' then 'SOLRUSHAPP'
		else 'UNKNOWN'
		end as platform
     , m.margin
	, sum(case when t.trans_type = 'SIGNUP' then amount else 0 end) * -1 as cef
	, sum(case when t.trans_type = 'SIGNUP' then amount else 0 end) * -1 + sum(case when t.trans_type = 'SIGNUP' then play_money_amount else 0 end) * -1 as tef
	, sum(case when t.trans_type = 'WINNINGS' then amount else 0 end) * -1 as winnings
	, sum(case when t.trans_type = 'SIGNUP' then amount else 0 end) * -1 * m.margin as nar
	, count(distinct case when date(u.createdate) = date(t.trans_date) and t.trans_type = 'SIGNUP' then u.user_id else null end) as new_players
	, count(distinct case when date(u.createdate) < date(t.trans_date) and t.trans_type = 'SIGNUP' then u.user_id else null end) as legacy_players
	, count(distinct case when t.trans_type = 'SIGNUP' then u.user_id else null end) as total_players
	, count(distinct case when date(u.createdate) = date(t.trans_date) and t.trans_type = 'SIGNUP' then t.int_trans_id else null end) as new_player_signups
	, count(distinct case when date(u.createdate) < date(t.trans_date) and t.trans_type = 'SIGNUP' then t.int_trans_id else null end) as legacy_player_signups
	, count(distinct case when t.trans_type = 'SIGNUP' then t.int_trans_id else null end) as total_signups
        , sum(t.amount) * -1 as nar_actual
        , count(distinct t.product_id) as tournaments
FROM ww.internal_transactions t
INNER JOIN ww.dim_users u ON t.user_id = u.user_id
INNER JOIN ww.tournament_games tg ON t.product_id = tg.tournament_id
INNER JOIN ww.agg_daily_margin m ON date(t.trans_date) = m.date AND tg.gametype_id = m.gametype_id
INNER JOIN ww.dim_gametypes g on tg.gametype_id = g.gametype_id
LEFT OUTER JOIN ww.clid_combos_ext c on t.client_id = c.client_id
WHERE t.trans_type in ('SIGNUP','WINNINGS')
  and t.trans_date >= '{{ ds }}'
GROUP BY 1,2,3,4,5;
