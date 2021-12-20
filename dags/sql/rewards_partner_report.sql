with users as
(select
u.partnertag as partner_id,
u.user_id as worldwinner_id,
advertiser_id,
balance
from ww.dim_users u
where advertiser_id = '21ww1'
)
select /*+ LABEL ('airflow-skill-skill_partner_reports_000-t_send_email_rewards_partner' )*/
distinct
date(trans_date) as date,
u.partner_id as daily_rewards_id,
u.worldwinner_id,
u.balance,
(sum(it.amount) + sum(it.play_money_amount))* -1 entry_fee_total,
sum(it.amount)*-1 entry_fee_real,
sum(it.play_money_amount)*-1 entry_fee_credit,
(sum(it.amount)*-1) * .0542 as revenue
from users u
   left join ww.internal_transactions it
   on u.worldwinner_id = it.user_id
where date(it.trans_date) = date('{{ ds }}')
and it.trans_type = 'SIGNUP'
group by 1,2,3,4;