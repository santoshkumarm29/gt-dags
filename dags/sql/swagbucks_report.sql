with users as
(select
u.partnertag as swagbucks_id,
u.user_id as worldwinner_id,
advertiser_id
from ww.dim_users u
where advertiser_id in ('21xku','21y8p')) --- US & Canada (Canada added 10/22/20)

select /*+ LABEL ('airflow-skill-skill_partner_reports_000-t_send_email_swagbucks' )*/
*,
ww_rev_today * .35 as total_revenue,
ww_rev_today * 4 as swagbucks --- changed from 12 to 4 10/21/20
from
(select
s.swagbucks_id,
s.worldwinner_id,
date(trans_date) as date,
sum(case when trans_type = 'SIGNUP' and date(trans_date) = date('{{ ds }}') then amount*-1 end) ww_rev_today
from users s
    left join ww.internal_transactions it
    on s.worldwinner_id = it.user_id
where trans_type  = 'SIGNUP'
group by 1,2,3) x
where ww_rev_today > 0;
