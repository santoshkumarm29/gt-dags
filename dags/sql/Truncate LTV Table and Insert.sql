truncate table ww.ltv_calculations_ods;

insert /*+ direct, LABEL('airflow-skill-{{ dag.dag_id }}-{{ task.task_id }}') */
    into ww.ltv_calculations_ods
    select distinct
              r.cohort_year
            , r.cohort_month
            , r.cohort_reg_platform
            , r.user_id
            , date(u.createdate) as reg_date
            , date(u.real_money_date) as ftd_date
            , datediff(day,r.createdate,ud.event_day) as days_since_reg
            , datediff(day,r.createdate,DATE('{{ tomorrow_ds }}') - 1) as days_since_reg_vs_yesterday
            , datediff(day,u.real_money_date,ud.event_day) as days_since_ftd
            , datediff(day,u.real_money_date,DATE('{{ tomorrow_ds }}') - 1) as days_since_ftd_vs_yesterday
            , case when u.advertiser_id 
            in ('21sgt','21sh4','21ww1','21wsb','21sip','21tya'
            ,'21xku','21y8p','21y8q','21ybf','21ybw','21yeb','21yf7'
            ,'21kb1','21dp7','21ij5','21tyb','21tyw')
                then 'Main Partners'
                else 'Other' 
                end as partner_category
            , u.advertiser_id
            , CASE 
                WHEN network_name IN ('unattributed','Long Tail Pilot') then 'Unattributed'
                WHEN network_name IN ('Branch.io - iOS','SmartLinks') then 'CRM'
                ELSE 'Paid'
                END AS ua_source
            , network_name
            , m.margin
            , sum(ud.ump_all_platforms) as umps
            , sum(ud.ump_desktop) as ump_desktop
            , sum(ud.ump_moweb) as ump_moweb
            , sum(ud.ump_phx) as ump_moapp
            , sum(ud.cef_all_platforms) as cef
            , sum(ud.cef_desktop) as cef_desktop
            , sum(ud.cef_moweb) as cef_moweb
            , sum(ud.cef_phx) as cef_moapp
            , sum(ud.cef_all_platforms) * m.margin as nar
            , sum(ud.cef_desktop) * m.margin as nar_desktop
            , sum(ud.cef_moweb) * m.margin as nar_moweb
            , sum(ud.cef_phx) * m.margin as nar_moapp
        from 
            (
            SELECT DISTINCT
                  extract(year from u.createdate) as cohort_year
                , extract(month from u.createdate) as cohort_month
                , u.createdate
                , u.real_money_date
                , s.network_name
                , CASE 
                    WHEN s.user_id IS NULL THEN 'DESKTOP'
                    ELSE 'APP'
                    END AS cohort_reg_platform 
                , u.user_id     
            FROM ww.dim_users u
            LEFT JOIN 
                (
                select distinct
                      k.idfv
                    , c.user_id
                    , c.createdate
                    , c.real_money_date
                    , k.network_name
                from gsnmobile.kochava_device_summary_phoenix k
                join 
                    (
                    select
                          b.idfv
                        , u.user_id
                        , u.real_money_date
                        , timestampadd(hour,3,b.event_time) as first_ww_app_login
                        , u.createdate
                    from (
                        select
                              idfv
                            , user_id
                            , event_time
                            , row_number() over (partition by idfv order by event_time asc) as rownum
                        from 
                            (
                            select
                                  idfv
                                , user_id
                                , min(event_time) as event_time
                            from phoenix.events_client
                            where user_id is not null
                            group by 1,2
                            order by 1,3
                            ) a
                        order by 1,3 asc
                        ) b
                    left join ww.dim_users u
                        on b.user_id = u.user_id
                    where b.rownum = 1
                    ) c
                    on k.idfv = c.idfv 
                where date(k.first_ww_app_login) <= date(k.first_ww_reg)
                and date(timestampadd(hour,3,k.first_ww_reg)) <= '2018-10-08'  

                union

                select distinct 
                     '0' as idfv
                    , a.user_id
                    , u.createdate
                    , u.real_money_date
                    , id.network_name
                from 
                   (
                    select distinct 
                          user_id
                        , platform_id
                        , date(activity_date) as day
                        , row_number() over (partition by user_id order by activity_date) as login_rank
                    from ww.login_activity
                    ) a 
                join ww.dim_users u
                    on a.user_id = u.user_id
                    and a.day = date(u.createdate)
                join ww.game_platforms gp
                    on a.platform_id = gp.platform_id
                left join 
                    (                
                    select distinct
                          e.idfv
                        , e.user_id
                        , k.network_name
                    from ww.dim_users u
                    join phoenix.events_client e
                        on u.user_id = e.user_id
                        and timestampadd(hour,3,e.event_time) >= timestampadd(hour,-1,u.createdate)
                        and timestampadd(hour,3,e.event_time) <= timestampadd(hour,1,u.createdate)
                    left join gsnmobile.kochava_device_summary_phoenix k
                        on e.idfv = k.idfv
                    ) id
                    on u.user_id = id.user_id
                where u.createdate >= '2018-10-09'
                and a.login_rank = 1
                and a.platform_id > 2 --IDs 1 and 2 are desktop and moweb, everything else is app
                ) s 
                on u.user_id = s.user_id
            ) r
        join ww.user_day ud
            on r.user_id = ud.user_id 
        join ww.dim_users u 
            on r.user_id = u.user_id
        join      
            (
            select distinct
                  date(trans_date) as day
                , (sum(case when trans_type = 'SIGNUP' then amount else 0 end) + sum(case when trans_type = 'WINNINGS' then amount else 0 end)) / sum(case when trans_type = 'SIGNUP' then amount else 0 end) as margin
            from ww.internal_transactions
            where trans_type in ('SIGNUP','WINNINGS')
            group by 1
            ) m
            on date(ud.event_day) = m.day
        where datediff(day,r.createdate,DATE('{{ tomorrow_ds }}') - 1) >= datediff(day,r.createdate,ud.event_day)
        and r.createdate >= '2011-01-01'
        and ud.event_day <= date('{{ tomorrow_ds }}')
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
        ;

-- swap ods with prod
ALTER TABLE ww.ltv_calculations, ww.ltv_calculations_ods RENAME TO ltv_calculations_old, ltv_calculations;
ALTER TABLE ww.ltv_calculations_old RENAME TO ltv_calculations_ods;
TRUNCATE TABLE ww.ltv_calculations_ods;
SELECT analyze_statistics('ww.ltv_calculations');
