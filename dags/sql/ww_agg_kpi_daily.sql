CREATE local TEMPORARY TABLE p1_games ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    user_id
    , date(finish_time) as p1_date
from (
    select distinct
        user_id
        , finish_time
        , row_number() over (partition by user_id order by finish_time) as game_rank
        from ww.played_games
    ) x
where game_rank = 1
and finish_time between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
;

CREATE local TEMPORARY TABLE registrations_pre ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select
 *,
 case when platform_id = 1 then 'Desktop'
      when platform_id = 2 then 'MoWeb'
      when platform_id >= 3 then 'App' end
      as reg_platform
 from
 (select distinct
        user_id
        , activity_date
        , platform_id
        , row_number() over (partition by user_id order by activity_date) as login_rank
    from ww.login_activity)x
    where login_rank = 1
;

CREATE local TEMPORARY TABLE users ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select
    user_id,
    date(createdate) as createdate,
    guest_play_account,
    date(guest_play_account_convert_date) as guest_play_account_convert_date,
    date(real_money_date) as real_money_date,
    case when guest_play_account is null then date(createdate)
         when guest_play_account is not null then date(guest_play_account_convert_date)
         end as regdate
    from
    ww.dim_users u
;

CREATE local TEMPORARY TABLE guest_accounts ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select
createdate as day,
count(case when guest_play_account is not null then u.user_id end) guest_account_created,
count(case when guest_play_account is not null and createdate = guest_play_account_convert_date then u.user_id end) as guest_accounts_same_day_reg
from users u
where createdate between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
group by 1
order by 1
;

CREATE local TEMPORARY TABLE registrations ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select
(regdate) as day,
count(u.user_id) as registrations,
count(case when reg_platform = 'Desktop' then u.user_id end) as registrations_desktop,
count(case when reg_platform = 'MoWeb' then u.user_id end) as registrations_moweb,
count(case when reg_platform = 'App' then u.user_id end) as registrations_app
--count(case when date(regdate) = date(real_money_date) then u.user_id end) as sameday_ftd_of_regs,
--count(case when date(createdate) = p1_date then p.user_id end) as sameday_npps_of_regs
from users u
    left join registrations_pre l
    on u.user_id = l.user_id
    left join p1_games p
    on u.user_id = p.user_id
where (regdate) between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
group by 1
order by 1
;

CREATE local TEMPORARY TABLE new_practice_players ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    p.p1_date as day
    , count(distinct p.user_id) as npps
    , count(distinct u.user_id) as same_day_npps
from p1_games p
left join ww.dim_users u
on p.user_id = u.user_id and p.p1_date = date(u.createdate)
group by 1
order by 1
;

CREATE local TEMPORARY TABLE ftds ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    date(real_money_date) as day
    , count(distinct user_id) as ftds
    , count(case when guest_play_Account is null and date(createdate) = date(real_money_date) then user_id -- dekstop/moweb/nonguest app users
                 when guest_play_account is not null and date(guest_play_account_convert_date) = date(real_money_date) then user_id end) as same_day_ftds -- guest app users
from ww.dim_users
where real_money_date is not null
and date(real_money_date) between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
group by 1
order by 1
;


CREATE local TEMPORARY TABLE daily_active_players ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    date(start_time) as day
    , count(distinct user_id) as daps
from ww.played_games
where start_time between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
group by 1
order by 1
;


CREATE local TEMPORARY TABLE daily_active_users ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    date(activity_date) as day
    , count(distinct user_id) as daus
    , count(distinct case when platform_id = 1 then user_id end) as daus_desktop
    , count(distinct case when platform_id = 2 then user_id end) as daus_moweb
    , count(distinct case when platform_id > 2 then user_id end) as daus_app
from ww.login_activity
where date(activity_date) between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
group by 1
order by 1
;


CREATE local TEMPORARY TABLE cash_free_cash_players ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    date(pg.start_time) as day
    , count(distinct pg.user_id) as cfps
from ww.played_games pg
join ww.dim_users u
on pg.user_id = u.user_id
left join ww.internal_transactions it
on pg.user_id = it.user_id and date(pg.start_time) = date(it.trans_date) and it.trans_type = 'SIGNUP'
where it.user_id is null
and date(pg.start_time) between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
and u.real_money_date is not null
group by 1
order by 1
;

CREATE local TEMPORARY TABLE cash_free_cash_players_by_platform ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    date(pg.start_time) as day
    , count(distinct case when pgp.platform_id = 1 then pg.user_id end) as cfps_desktop
    , count(distinct case when pgp.platform_id = 2 then pg.user_id end) as cfps_moweb
    , count(distinct case when pgp.platform_id >= 3 then pg.user_id end) as cfps_app
    , count(distinct case when pgp.platform_id is null then pg.user_id end) as cfps_unknown
    , count(distinct pg.user_id) as cfps
from ww.played_games pg
join ww.dim_users u
on pg.user_id = u.user_id
left join ww.internal_transactions it
on pg.user_id = it.user_id and date(pg.start_time) = date(it.trans_date) and it.trans_type = 'SIGNUP'
left join ww.played_game_platform pgp --- an inner join removes the null platforms
on pg.game_id = pgp.game_id
where it.user_id is null
and date(pg.start_time) between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
and u.real_money_date is not null
group by 1
order by 1
;

CREATE local TEMPORARY TABLE chum ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select
date(trans_date) as day,
count(distinct user_id) chum_users
from ww.internal_transactions
where trans_type = 'BNSBCKS-CR'
and description like '%CHUM%'
and date(trans_date) between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
group by 1
order by 1
;

CREATE local TEMPORARY TABLE free_practice_players ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    date(pg.start_time) as day
    , count(distinct pg.user_id) as practice_players
from ww.played_games pg
join ww.dim_users u
on pg.user_id = u.user_id
left join ww.internal_transactions it
on u.user_id = it.user_id
and date(pg.start_time) = date(it.trans_date)
and it.trans_type = 'SIGNUP'
and (u.real_money_date is null or date(u.real_money_date) > date(it.trans_date))
where it.user_id is null
and (u.real_money_date is null or date(u.real_money_date) > date(pg.start_time))
and date(pg.start_time) between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
group by 1
order by 1
;



CREATE local TEMPORARY TABLE cash_stats ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    date(it.trans_date) as day
    , count(distinct case when it.trans_type = 'SIGNUP' then it.user_id end) as umps
    , count(distinct case when it.trans_type = 'SIGNUP' then it.int_trans_id end) as signups
    , sum(case when it.trans_type = 'SIGNUP' then it.amount else 0 end) * -1 as cef
    , sum(case when it.trans_type = 'SIGNUP' then it.play_money_amount else 0 end) * -1 as pef
    , sum(case when it.trans_type = 'SIGNUP' then it.deposit_credits_amount else 0 end) * -1 as bbef
    , sum(case when it.trans_type = 'WINNINGS' then it.amount else 0 end) * -1 as winnings
    , sum(case when it.trans_type ilike '%deposit%' then it.amount else 0 end) as deposits
    , count(distinct case when it.trans_type ilike '%deposit%' then it.user_id end) as depositors
    , sum(case when it.trans_type ilike '%withdraw%' then it.amount else 0 end) as withdrawals
    --
    , count(distinct case when c.device is not null and c.platform = 'skill-app' and c.operating_system ilike '%ios%' and it.trans_type = 'SIGNUP' then it.user_id end) as umps_app_ios
    , count(distinct case when c.device is not null and c.platform = 'skill-app' and c.operating_system ilike '%ios%' and it.trans_type = 'SIGNUP' then it.int_trans_id end) as signups_app_ios
    , sum(case when c.device is not null and c.platform = 'skill-app' and c.operating_system ilike '%ios%' and it.trans_type = 'SIGNUP' then it.amount else 0 end) * -1 as cef_app_ios
    , sum(case when c.device is not null and c.platform = 'skill-app' and c.operating_system ilike '%ios%' and it.trans_type = 'SIGNUP' then it.play_money_amount else 0 end) * -1 as pef_app_ios
    , sum(case when c.device is not null and c.platform = 'skill-app' and c.operating_system ilike '%ios%' and it.trans_type = 'SIGNUP' then it.deposit_credits_amount else 0 end) * -1 as bbef_app_ios
    , sum(case when c.device is not null and c.platform = 'skill-app' and c.operating_system ilike '%ios%' and it.trans_type ilike '%deposit%' then it.amount else 0 end) as deposits_app_ios
    , count(distinct case when c.device is not null and c.platform = 'skill-app' and c.operating_system ilike '%ios%' and it.trans_type ilike '%deposit%' then it.user_id end) as depositors_app_ios
    , sum(case when c.device is not null and c.platform = 'skill-app' and c.operating_system ilike '%ios%' and it.trans_type ilike '%withdraw%' then it.amount else 0 end) as withdrawals_app_ios
    --
    , count(distinct case when c.device is not null and c.platform = 'skill-app' and c.operating_system not ilike '%ios%' and it.trans_type = 'SIGNUP' then it.user_id end) as umps_app_android
    , count(distinct case when c.device is not null and c.platform = 'skill-app' and c.operating_system not ilike '%ios%' and it.trans_type = 'SIGNUP' then it.int_trans_id end) as signups_app_android
    , sum(case when c.device is not null and c.platform = 'skill-app' and c.operating_system not ilike '%ios%' and it.trans_type = 'SIGNUP' then it.amount else 0 end) * -1 as cef_app_android
    , sum(case when c.device is not null and c.platform = 'skill-app' and c.operating_system not ilike '%ios%' and it.trans_type = 'SIGNUP' then it.play_money_amount else 0 end) * -1 as pef_app_android
    , sum(case when c.device is not null and c.platform = 'skill-app' and c.operating_system not ilike '%ios%' and it.trans_type = 'SIGNUP' then it.deposit_credits_amount else 0 end) * -1 as bbef_app_android
    , sum(case when c.device is not null and c.platform = 'skill-app' and c.operating_system not ilike '%ios%' and it.trans_type ilike '%deposit%' then it.amount else 0 end) as deposits_app_android
    , count(distinct case when c.device is not null and c.platform = 'skill-app' and c.operating_system not ilike '%ios%' and it.trans_type ilike '%deposit%' then it.user_id end) as depositors_app_android
    , sum(case when c.device is not null and c.platform = 'skill-app' and c.operating_system not ilike '%ios%' and it.trans_type ilike '%withdraw%' then it.amount else 0 end) as withdrawals_app_android
    --
    , count(distinct case when c.device is null and it.trans_type = 'SIGNUP' then it.user_id end) as umps_desktop
    , count(distinct case when c.device is null and it.trans_type = 'SIGNUP' then it.int_trans_id end) as signups_desktop
    , sum(case when c.device is null and it.trans_type = 'SIGNUP' then it.amount else 0 end) * -1 as cef_desktop
    , sum(case when c.device is null and it.trans_type = 'SIGNUP' then it.play_money_amount else 0 end) * -1 as pef_desktop
    , sum(case when c.device is null and it.trans_type = 'SIGNUP' then it.deposit_credits_amount else 0 end) * -1 as bbef_desktop
    , sum(case when c.device is null and it.trans_type ilike '%deposit%' then it.amount else 0 end) as deposits_desktop
    , count(distinct case when c.device is null and it.trans_type ilike '%deposit%' then it.user_id end) as depositors_desktop
    , sum(case when c.device is null and it.trans_type ilike '%withdraw%' then it.amount else 0 end) as withdrawals_desktop
    --
    , count(distinct case when c.device is not null and (c.platform not in ('skill-app', 'skill-solrush') or c.platform is null) and trans_type = 'SIGNUP' then it.user_id end) as umps_moweb
    , count(distinct case when c.device is not null and (c.platform not in ('skill-app', 'skill-solrush') or c.platform is null) and trans_type = 'SIGNUP' then it.int_trans_id end) as signups_moweb
    , sum(case when c.device is not null and (c.platform not in ('skill-app', 'skill-solrush') or c.platform is null) and trans_type = 'SIGNUP' then it.amount else 0 end) * -1 as cef_moweb
    , sum(case when c.device is not null and (c.platform not in ('skill-app', 'skill-solrush') or c.platform is null) and trans_type = 'SIGNUP' then it.play_money_amount else 0 end) * -1 as pef_moweb
    , sum(case when c.device is not null and (c.platform not in ('skill-app', 'skill-solrush') or c.platform is null) and trans_type = 'SIGNUP' then it.deposit_credits_amount else 0 end) * -1 as bbef_moweb
    , sum(case when c.device is not null and (c.platform not in ('skill-app', 'skill-solrush') or c.platform is null) and trans_type ilike '%deposit%' then it.amount else 0 end) as deposits_moweb
    , count(distinct case when c.device is not null and (c.platform not in ('skill-app', 'skill-solrush') or c.platform is null) and trans_type ilike '%deposit%' then it.user_id end) as depositors_moweb
    , sum(case when c.device is not null and (c.platform not in ('skill-app', 'skill-solrush') or c.platform is null) and trans_type ilike '%withdraw%' then it.amount else 0 end) as withdrawals_moweb
from ww.internal_transactions it
left join ww.clid_combos_ext c
on it.client_id = c.client_id
where (it.trans_type in ('SIGNUP','WINNINGS') or it.trans_type ilike '%deposit%' or it.trans_type ilike '%withdraw%')
and it.trans_date between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
and it.trans_date < (DATE '{{ ds }}' + 1)
group by 1
order by 1
;


CREATE local TEMPORARY TABLE depositors_100_plus ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    day
    , count(distinct user_id) as depositors_100_plus
from (
    select distinct
        date(trans_date) as day
        , user_id
        , sum(amount) as total_deposits
    from ww.internal_transactions
    where trans_type ilike '%deposit%' 
    and date(trans_date)  between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
    group by 1,2
) x
where total_deposits >= 100
group by 1
order by 1
;


CREATE local TEMPORARY TABLE deposits ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    it.user_id
    , it.trans_date
    , it.int_trans_id
    , it.amount
    , case
        when c.device is null then 'desktop'
        when c.device is not null
            and (c.platform not in ('skill-app', 'skill-solrush') or c.platform is null) then 'moweb'
    when c.device is not null
        and c.platform = 'skill-app' and c.operating_system ilike '%ios%' then 'moapp - iphone'
    when c.device is not null
        and c.platform = 'skill-app' and c.operating_system not ilike '%ios%' then 'moapp - android'
    when c.device is not null
        and c.platform = 'skill-solrush' then 'solrushapp'
    else 'unknown'
    end as platform
    , row_number() over (partition by it.user_id order by it.trans_date) as deposit_rank
from ww.internal_transactions it
join ww.dim_users u
on it.user_id = u.user_id
left join ww.clid_combos_ext c
on it.client_id = c.client_id
where it.trans_type ilike '%deposit%' 
and date(u.real_money_date)  between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
and it.trans_date < (DATE '{{ ds }}' + 1)
;


CREATE local TEMPORARY TABLE ftds_by_platform ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    date(trans_date) as day
    , count(distinct case when platform = 'desktop' then user_id end) as ftds_desktop
    , count(distinct case when platform = 'moweb' then user_id end) as ftds_moweb
    , count(distinct case when platform = 'moapp - iphone' then user_id end) as ftds_app_ios
    , count(distinct case when platform = 'moapp - android' then user_id end) as ftds_app_android
from deposits
where deposit_rank = 1
group by 1
order by 1
;


CREATE local TEMPORARY TABLE second_depositors_through_day_n ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
     date(u.real_money_date) as day
    , count(distinct case when u.real_money_date < ((date(trans_date) + 1) - 7) and datediff(day,u.real_money_date,d.trans_date) <= 7 then d.user_id end) as second_depositors_7d
    , count(distinct case when u.real_money_date < ((date(trans_date) + 1) - 30) and datediff(day,u.real_money_date,d.trans_date) <= 30 then d.user_id end) as second_depositors_30d
from ww.dim_users u
left join deposits d
on u.user_id = d.user_id
and d.deposit_rank = 2
and datediff(day,u.real_money_date,d.trans_date) <= 30
where u.real_money_date is not null
and date(u.real_money_date)  between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
group by 1
order by 1
;


CREATE local TEMPORARY TABLE net_deposits_through_day_n ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    date(u.real_money_date) as day
    , sum(case when u.real_money_date < ((date(it.trans_date) + 1) - 7) and date(it.trans_date) <= date(u.real_money_date) + 7 then it.amount end) as net_deposits_7d
    , sum(case when u.real_money_date < ((date(it.trans_date) + 1) - 30) and date(it.trans_date) <= date(u.real_money_date) + 30 then it.amount end) as net_deposits_30d
from ww.dim_users u
join ww.internal_transactions it
on u.user_id = it.user_id
where date(it.trans_date) <= date(u.real_money_date) + 30
and it.trans_date < (DATE '{{ ds }}' + 1)
and (it.trans_type ilike '%deposit%' or it.trans_type ilike '%withdraw%')
and date(u.real_money_date)  between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
group by 1
order by 1
;


CREATE local TEMPORARY TABLE payer_retention ON COMMIT preserve rows direct AS
/*+ LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
select distinct
    date(u.real_money_date) as day
    , count(distinct case when datediff(day,u.real_money_date,(DATE '{{ ds }}' + 1) - 1) >= 14 then u.user_id end) payer_retention_denom_w2
    , count(distinct case when datediff(day,u.real_money_date,(DATE '{{ ds }}' + 1) - 1) >= 21 then u.user_id end) payer_retention_denom_w3
    , count(distinct case when datediff(day,u.real_money_date,(DATE '{{ ds }}' + 1) - 1) >= 28 then u.user_id end) payer_retention_denom_w4
    , count(distinct case when datediff(day,u.real_money_date,(DATE '{{ ds }}' + 1) - 1) >= 14 and datediff(day,u.real_money_date,it.trans_date) > 7 and datediff(day,u.real_money_date,it.trans_date) <= 14 then u.user_id end) as payer_retention_num_w2
    , count(distinct case when datediff(day,u.real_money_date,(DATE '{{ ds }}' + 1) - 1) >= 21 and datediff(day,u.real_money_date,it.trans_date) > 14 and datediff(day,u.real_money_date,it.trans_date) <= 21 then u.user_id end) as payer_retention_num_w3
    , count(distinct case when datediff(day,u.real_money_date,(DATE '{{ ds }}' + 1) - 1) >= 28 and datediff(day,u.real_money_date,it.trans_date) > 21 and datediff(day,u.real_money_date,it.trans_date) <= 28 then u.user_id end) as payer_retention_num_w4
from ww.dim_users u
left join ww.internal_transactions it
on u.user_id = it.user_id
and it.trans_date >= u.real_money_date
and it.trans_type = 'SIGNUP'
where date(u.real_money_date)  between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
group by 1
order by 1
;

DELETE /*+ direct, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */ FROM ww.agg_kpi_daily
where day between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
;

INSERT /*+ direct, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */ INTO ww.agg_kpi_daily
select distinct
d.thedate as day
--, v.visitors as web_visitors see BI-13331
, null as web_visitors
, r.registrations as ww_new_regs
, r.registrations_app
, n.npps
, n.same_day_npps
, f.ftds
, fp.ftds_desktop
, fp.ftds_moweb
, fp.ftds_app_ios
, fp.ftds_app_android
, dp.daps
, du.daus
, du.daus_desktop
, du.daus_moweb
, du.daus_app
, cf.cfps
, p.practice_players
, c.umps
, c.signups
, c.cef
, c.pef
, c.bbef
, c.cef + c.pef + c.bbef as tef
, c.winnings
, c.deposits
, c.depositors
, c.cef + c.winnings as nar
, (c.cef + c.winnings) / c.cef as margin
, c.withdrawals
, c.umps_desktop
, c.umps_moweb
, c.umps_app_ios
, c.umps_app_android
, c.signups_desktop
, c.signups_moweb
, c.signups_app_ios
, c.signups_app_android
, c.cef_desktop
, c.cef_moweb
, c.cef_app_ios
, c.cef_app_android
, c.pef_desktop
, c.pef_moweb
, c.pef_app_ios
, c.pef_app_android
, c.bbef_desktop
, c.bbef_moweb
, c.bbef_app_ios
, c.bbef_app_android
, c.deposits_desktop
, c.deposits_moweb
, c.deposits_app_ios
, c.deposits_app_android
, c.depositors_desktop
, c.depositors_moweb
, c.depositors_app_ios
, c.depositors_app_android
, c.withdrawals_desktop
, c.withdrawals_moweb
, c.withdrawals_app_ios
, c.withdrawals_app_android
, dd.depositors_100_plus
, s.second_depositors_7d
, nd.net_deposits_7d
, s.second_depositors_30d
, nd.net_deposits_30d
, pr.payer_retention_num_w2
, pr.payer_retention_denom_w2
, pr.payer_retention_num_w3
, pr.payer_retention_denom_w3
, pr.payer_retention_num_w4
, pr.payer_retention_denom_w4
, r.registrations_desktop
, r.registrations_moweb
, g.guest_account_created
, g.guest_accounts_same_day_reg
, f.same_day_ftds
, cfp.cfps_desktop
, cfp.cfps_moweb
, cfp.cfps_app
, ch.chum_users
, c.deposits/c.depositors as deposit_per_depositor
, c.deposits_desktop/c.depositors_desktop as deposit_per_depositor_desktop
, c.deposits_moweb/c.depositors_moweb as deposit_per_depositor_moweb
, c.deposits_app_ios/c.depositors_app_ios as deposit_per_depositor_app_ios
, c.deposits_app_android/c.depositors_app_android as deposit_per_depositor_app_android
from newapi.dim_date d
--    join visitors.unique_visitors v
--    on d.thedate = v.event_day
    join registrations r
    on d.thedate = r.day
    join guest_accounts g
    on d.thedate = g.day
    join new_practice_players n
    on d.thedate = n.day
    join ftds f
    on d.thedate = f.day
    join ftds_by_platform fp
    on d.thedate = fp.day
    join daily_active_players dp
    on d.thedate = dp.day
    join daily_active_users du
    on d.thedate = du.day
    join cash_free_cash_players cf
    on d.thedate = cf.day
    join cash_free_cash_players_by_platform cfp
    on d.thedate = cfp.day
    join chum ch
    on d.thedate = ch.day
    join free_practice_players p
    on d.thedate = p.day
    join cash_stats c
    on d.thedate = c.day
    join depositors_100_plus dd
    on d.thedate = dd.day
    join second_depositors_through_day_n s
    on d.thedate = s.day
    join net_deposits_through_day_n nd
    on d.thedate = nd.day
    join payer_retention pr
    on d.thedate = pr.day
where d.thedate between (DATE '{{ ds }}' + 1) - 35 and (DATE '{{ ds }}' + 1)
order by thedate
;

commit;