select  /*+ LABEL ('airflow-all-etl_gsn_daily_kpi_email-t_validate_qa_checks')*/
case when sum(flag) = 0 then 1
else 0 end as valid
from
(
select
name, noads, level, mtd, variable, yesterday_val, today_val, pct_change, abs_change,
case
  -- if both today and yesterday's values were 0, no sudden drop detected to flag for errors
  when nvl(yesterday_val,0) = 0 and nvl(today_val,0) = 0 then 0
  -- if yesterday had a value and today's value is 0 or null it could indicate a sudden change to investigate
  when yesterday_val > 0 and nvl(today_val,0) = 0
    then
      case
        -- ignore these because they are often low sample and can drop to 0 for smaller apps
        -- cpi only available sometimes in later email
        when variable in ('curr_day7_rr', 'curr_day30_rr','delta_day1_rr','delta_day7_rr','delta_day30_rr','cpi','cpi_rr','delta_ad_rev') then 0
        -- ignore ad revenue that may be reported for apps that have no budget
        when variable = 'curr_ad_rev' and (level = 'studio' or noads = 'True' or name = 'Grand Casino') then 0
      else 1 end
   -- commenting out for now bc these are too noisy
  -- if either budget tracking % swings by an absolute amount of +/- 5%
--  when variable='ad_percent_to_budget' and abs_change > 0.05 then 1
--  when variable='iap_percent_to_budget' and abs_change > 0.05 then 1
  -- if various metrics change by a large % swing compared to yesterday
  -- use minimum value thresholds to guard against apps with low KPIs (which could mean high % swings)
--  when variable='curr_bookings' and mtd = false and (pct_change >= 1.5 or pct_change <= 0.5) then 1
--  when variable='curr_bookings' and mtd = true and (pct_change >= 1.1 or pct_change <= 0.9) then 1
--  when variable='curr_dau' and (pct_change <=  0.95 or pct_change >= 1.05) then 1
--  when variable='curr_installs' and today_val >= 1500 and (pct_change <=  0.90 or pct_change >= 1.1) then 1
--  when variable='curr_payers' and today_val >= 1000 and (pct_change <=  0.75 or pct_change >= 1.25) then 1
--  when variable='curr_pct_payers' and abs_change > 0.03 then 1
--  when variable='curr_rolling_arpdau' and (pct_change <=  0.75 or pct_change >= 1.25) then 1
  else 0 end as flag from
(select name, noads, level, mtd, variable, yesterday_val, today_val,
today_val/(case when yesterday_val=0 then 1 else yesterday_val end) as pct_change, abs(today_val - yesterday_val) as abs_change
from (
select name, max(case when event_day = '{{ ds }}' then noads else null end) as noads, level, mtd, variable,
max(case when event_day = '{{ yesterday_ds }}' then value else null end) as yesterday_val,
max(case when event_day = '{{ ds }}' then value else null end) as today_val
 from kpi.daily_email_output_latest_v
group by 1,3,4,5) x
) x
-- these apps are small and unpredictable
where name not in ('Fresh Deck Poker','Wheel of Fortune Slots 2.0', 'Game Taco')
) x;
