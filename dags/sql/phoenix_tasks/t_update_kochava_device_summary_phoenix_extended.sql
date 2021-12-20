
CREATE local temp TABLE temp_device_hardware_joins ON COMMIT PRESERVE ROWS AS
/*+ direct,LABEL ('airflow-ua-ua_master_main_dag.phoenix_subdag-t_update_kochava_device_summary_phoenix_extended')*/
(select distinct
         device_original,
         model,
         UPPER(
         REPLACE(
         case when model in ('iPhone', 'iPad', 'iPod') then device
              when device in ('KFTT', 'KFSOWI') then 'KFTT, KFSOWI'
              when device in ('KFOT', 'Kindle Fire') then 'KFOT, Kindle Fire'
              when left(device,8) = 'SAMSUNG-' then replace(device,'SAMSUNG-','')
              when device = 'Moto E (4)' then 'XT1768'
              when device = 'XT1710-02' then 'MOTO Z (2)'
              when device = 'SM-G360T1' then 'SM-G360FY'
              when device = 'GT-P5113' then 'GT-P5110'
              when device = 'SGH-T999' then 'SGH-T999L'
              when device = 'SM-J510FN' then 'SM-J510FN/DS'
              when device = 'QMV7B' then 'QMV7A'
              when device = 'VS985 4G' then 'VS985'
              when device = 'HTC One' then 'HTC6500LVW'
              when device = 'LGL34C' then 'VS415PP'
              when device = 'HTC One_M8' then 'HTC6525LVW'
              WHEN left(device,7) in ('LM-G710','LM-Q610','LM-Q617','LM-Q710','LM-Q850','LM-X210','LM-X410') then left(device,7)
              when device = 'LM-Q710.FG' then 'LMQ710'
              when device = 'LM-V350' then 'VS501'
              when device = 'LG-M255' then 'V350N'
              when device = 'Nexus 5' then 'EM01L'
              when device = 'SM-J320FN' then 'SM-J320FN/DD'
              when device = 'SM-J320DD' then 'SM-J320FN/DD'
              when device = 'SM-J530F' then 'SM-J530F/DS'
              when device = 'SM-J530FM' then 'SM-J530FM/DS'
              when device = 'SM-T387V' then 'SM-T385'
              when device = 'SM-S727VL' then 'SM-J727A'
         else device end
         , '-','')
         ) as device_join,
         UPPER(
         REPLACE(
         case when length(device) >= 1 then left(device, length(device)-1) else 'NOJOIN1' end
         , '-','')
         ) as device_join2,
         UPPER(
         REPLACE(
         case when length(device) >= 2 then left(device, length(device)-2) else 'NOJOIN1' end
         , '-','')
         ) as device_join3
 from
 (select distinct model, device as device_original,
 case when position(' ' in device) > 0 then right(device, length(device)-position(' ' in device)) else device end as device
 from gsnmobile.kochava_device_summary_phoenix) a
 where case when coalesce(device,'') = '' then 1=0 else 1=1 end
);


CREATE local temp TABLE temp_mobile_device_data_joins ON COMMIT PRESERVE ROWS AS
/*+ direct,LABEL ('airflow-ua-ua_master_main_dag.phoenix_subdag-t_update_kochava_device_summary_phoenix_extended')*/
(
select brand, model, model_code, released, code_name, oem_id, category, cpu_speed,
       display_resolution, display_diagonal_mm, display_diagonal_in, id, release_date, device_join
from
(
        select *, row_number() over (partition by device_join order by release_date) as rn
        from
        (
                select
                        *,
                        (right(released,3) || ' 1, '|| left(released,4))::date as release_date,
                        UPPER(
                        REPLACE(
                        case when brand = 'Amazon' and model = 'Kindle Fire 8.9 HDX WiFi 32GB' then 'KFAPWI'
                             when brand = 'Amazon' and model = 'Kindle Fire 8.9 HDX 4G LTE 32GB' then 'KFAPWA'
                             when brand = 'Amazon' and model = 'Kindle Fire 7 HDX 4G LTE 32GB' then 'KFTHWA'
                             when brand = 'Amazon' and model = 'Kindle Fire 7 HDX WiFi 32GB' then 'KFTHWI'
                             when brand = 'Amazon' and model = 'Kindle Fire 8.9 HD 4G LTE 64GB' then 'KFJWA'
                             when brand = 'Amazon' and model = 'Kindle Fire 8.9 HD 32GB' then 'KFJWI'
                             when brand = 'Amazon' and model = 'Kindle Fire 7 HD 32GB' then 'KFTT, KFSOWI'
                             when brand = 'Amazon' and model = 'Kindle Fire' then 'KFOT, Kindle Fire'
                             when brand = 'Amazon' then oem_id
                             when brand = 'Apple' then replace(replace(code_name,'Apple ', ''),' ','')
                             when brand = 'LG' and left(model,4) = 'Leon' then 'LGMS345'
                             when brand = 'LG' and left(model,4) = 'K330' then 'LGMS330'
                             when brand = 'LG' and left(model,8) = 'LMX210CM' then 'LM-X210CM'
                             when brand = 'LG' and model_code in ('EM01L', 'V350N') then model_code
                             when left(model,6) in ('LMG710','LMQ610','LMQ617','LMQ710','LMQ850','LMX210','LMX410') then left(model,6)
                             when brand = 'ZTE' and model = 'ZMax Pro LTE' then 'Z981'
                             when model = 'Z982 Blade Z Max LTE-A' then 'Z982'
                             when left(model, 7) = 'Nexus 7' then 'Nexus 7'
                             when model = 'Galaxy Tab 3 7.0 WiFi 8GB' then 'SM-T210R'
                             when model = 'Galaxy Tab 4 7.0 TV WiFi / SM-T230NU' then 'SM-T230NU'
                             when model = 'Galaxy Tab 4 NOOK 10.1' then 'SM-T530NU'
                             when model = 'Galaxy Tab E Nook Edition 9.6' then 'SM-T560NU'
                             when brand = 'ZTE' and left(model,4) = 'Z970' then 'Z970'
                             when model = 'Ellipsis 7 4G XLTE QMV7A / QMV7B' then 'QMV7A'
                             when model = 'Ellipsis 8 4G XLTE QTAQZ3' then 'QTAQZ3'
                             when brand = 'Blackview' and model in ('A7 Dual Sim LTE','A7 Pro Dual Sim LTE') then 'A7'
                             when position(' ' in model) > 0 and left(model, -1+position(' ' in model)) in ('LMG710VM','LMX410UM','Z971','Z839','Z835','Z983','Z965','Z956','Z959','Z831','Z832','Z958','Z828','Z988','Z851','Z812','Z999','K81') then left(model, -1+position(' ' in model))
                             when left(model, 7) = 'Moto G4' then 'MOTO G (4)'
                             when left(model, 12) = 'Moto G6 Plus' then 'MOTO G (5) PLUS'
                             when left(model, 7) = 'Moto G5' then 'MOTO G (5)'
                             when left(model, 12) = 'Moto G6 Play' then 'MOTO G(6) PLAY'
                             when left(model, 12) = 'Moto G6 Plus' then 'MOTO G(6) PLUS'
                             when left(model, 13) = 'Moto G6 Forge' then 'MOTO G(6) FORGE'
                             when left(model, 7) = 'Moto G6' then 'MOTO G(6)'
                             when model = 'Moto E5 Play LTE US 16GB / Moto E5 Cruise' then 'MOTO E5 CRUISE'
                             when left(model, 12) = 'Moto E5 Play' then 'MOTO E5 PLAY'
                             when left(model, 12) = 'Moto E5 Plus' then 'MOTO E5 PLUS'
                             when left(model, 13) = 'Moto E5 Supra' then 'MOTO E5 SUPRA'
                             when left(model, 7) = 'Moto E5' then 'MOTO E5'
                             when left(model, 12) = 'Moto Z3 Play' then 'MOTO Z3 PLAY'
                             when left(model, 7) = 'Moto Z3' then 'MOTO Z3'
                             when left(model, 12) = 'Moto Z2 Play' then 'MOTO Z2 PLAY'
                             when left(model, 7) = 'Moto Z2' then 'MOTO Z (2)'
                             when model = 'Moto Z Droid Edition XLTE 32GB' then 'XT1650'
                             when left(model, 11) = 'Moto C Plus' then 'MOTO C PLUS'
                             when left(model, 6) = 'Moto C' then 'MOTO C'
                             when brand = 'LG' and coalesce(model_code,'') <> '' and left(model,2) not in ('VS', 'VK') and left(model_code,2) not in ('VS', 'VK') then 'LG' || model_code
                             when brand = 'LG' and coalesce(model_code,'') = '' and left(model,1) = 'M' then 'LG' || left(model, position(' ' in model || ' ')-1)
                             else model_code end
                        , '-','')
                        ) as device_join


                from apis.dim_mobile_device_info
        ) x
) y
where rn = 1
);

TRUNCATE TABLE gsnmobile.kochava_device_summary_phoenix_extended_ods;

INSERT /*+ direct,LABEL ('airflow-ua-ua_master_main_dag.phoenix_subdag-t_update_kochava_device_summary_phoenix_extended')*/
       INTO gsnmobile.kochava_device_summary_phoenix_extended_ods
SELECT
    x.idfv,
    x.platform,
    x.install_timestamp,
    x.install_day,
    x.app_name,
    x.campaign_name,
    x.campaign_id,
    x.site_id,
    x.network_name,
    x.creative,
    x.country_code,
    x.request_campaign_id,
    x.request_adgroup,
    x.request_keyword,
    x.request_matchtype,
    x.model,
    x.device,
    x.most_recent_os_version,
    x.first_payment_timestamp,
    x.last_payment_timestamp,
    x.first_active_day,
    x.last_active_day,
    x.first_ww_app_login,
    x.first_ww_reg,
    x.todate_bookings,
    x.payer,
    x.day1_bookings,
    x.day2_bookings,
    x.day3_bookings,
    x.day7_bookings,
    x.day14_bookings,
    x.day30_bookings,
    x.day60_bookings,
    x.day90_bookings,
    x.day180_bookings,
    x.day360_bookings,
    x.day540_bookings,
    x.day735_bookings,
    x.day1_payer,
    x.day2_payer,
    x.day3_payer,
    x.day7_payer,
    x.day14_payer,
    x.day30_payer,
    x.day60_payer,
    x.day90_payer,
    x.day180_payer,
    x.day360_payer,
    x.day540_payer,
    x.day735_payer,
    x.first_cef_timestamp,
    x.last_cef_timestamp,
    x.todate_cef,
    x.cash_entrant,
    x.day1_cef,
    x.day2_cef,
    x.day3_cef,
    x.day7_cef,
    x.day14_cef,
    x.day30_cef,
    x.day60_cef,
    x.day90_cef,
    x.day180_cef,
    x.day360_cef,
    x.day540_cef,
    x.day735_cef,
    x.day1_cash_entrant,
    x.day2_cash_entrant,
    x.day3_cash_entrant,
    x.day7_cash_entrant,
    x.day14_cash_entrant,
    x.day30_cash_entrant,
    x.day60_cash_entrant,
    x.day90_cash_entrant,
    x.day180_cash_entrant,
    x.day360_cash_entrant,
    x.day540_cash_entrant,
    x.day735_cash_entrant,
    x.first_deposit_timestamp,
    x.last_deposit_timestamp,
    x.todate_deposits,
    x.todate_deposit_count,
    x.depositor,
    x.day1_deposits,
    x.day2_deposits,
    x.day3_deposits,
    x.day7_deposits,
    x.day14_deposits,
    x.day30_deposits,
    x.day60_deposits,
    x.day90_deposits,
    x.day120_deposits,
    x.day150_deposits,
    x.day180_deposits,
    x.day210_deposits,
    x.day240_deposits,
    x.day270_deposits,
    x.day300_deposits,
    x.day330_deposits,
    x.day360_deposits,
    x.day540_deposits,
    x.day735_deposits,
    x.first_withdrawal_timestamp,
    x.last_withdrawal_timestamp,
    x.todate_withdrawals,
    x.todate_withdrawal_count,
    x.withdrawer,
    x.day1_withdrawals,
    x.day2_withdrawals,
    x.day3_withdrawals,
    x.day7_withdrawals,
    x.day14_withdrawals,
    x.day30_withdrawals,
    x.day60_withdrawals,
    x.day90_withdrawals,
    x.day120_withdrawals,
    x.day150_withdrawals,
    x.day180_withdrawals,
    x.day210_withdrawals,
    x.day240_withdrawals,
    x.day270_withdrawals,
    x.day300_withdrawals,
    x.day330_withdrawals,
    x.day360_withdrawals,
    x.day540_withdrawals,
    x.day735_withdrawals,
    x.day1_retained,
    x.day2_retained,
    x.day3_retained,
    x.day7_retained,
    x.day14_retained,
    x.day30_retained,
    x.day60_retained,
    x.day90_retained,
    x.day180_retained,
    x.day360_retained,
    x.day540_retained,
    x.day735_retained,
    x.kochava_install_timestamp,
    x.kochava_install_day,
    x.gsn_install_timestamp,
    x.gsn_install_day,
    x.day1_bookings_app,
    x.day2_bookings_app,
    x.day3_bookings_app,
    x.day7_bookings_app,
    x.day14_bookings_app,
    x.day30_bookings_app,
    x.day60_bookings_app,
    x.day90_bookings_app,
    x.day180_bookings_app,
    x.day360_bookings_app,
    x.day540_bookings_app,
    x.day735_bookings_app,
    x.day1_bookings_moweb,
    x.day2_bookings_moweb,
    x.day3_bookings_moweb,
    x.day7_bookings_moweb,
    x.day14_bookings_moweb,
    x.day30_bookings_moweb,
    x.day60_bookings_moweb,
    x.day90_bookings_moweb,
    x.day180_bookings_moweb,
    x.day360_bookings_moweb,
    x.day540_bookings_moweb,
    x.day735_bookings_moweb,
    x.day1_bookings_desktop,
    x.day2_bookings_desktop,
    x.day3_bookings_desktop,
    x.day7_bookings_desktop,
    x.day14_bookings_desktop,
    x.day30_bookings_desktop,
    x.day60_bookings_desktop,
    x.day90_bookings_desktop,
    x.day180_bookings_desktop,
    x.day360_bookings_desktop,
    x.day540_bookings_desktop,
    x.day735_bookings_desktop,
    x.day1_payer_app,
    x.day2_payer_app,
    x.day3_payer_app,
    x.day7_payer_app,
    x.day14_payer_app,
    x.day30_payer_app,
    x.day60_payer_app,
    x.day90_payer_app,
    x.day180_payer_app,
    x.day360_payer_app,
    x.day540_payer_app,
    x.day735_payer_app,
    x.day1_payer_moweb,
    x.day2_payer_moweb,
    x.day3_payer_moweb,
    x.day7_payer_moweb,
    x.day14_payer_moweb,
    x.day30_payer_moweb,
    x.day60_payer_moweb,
    x.day90_payer_moweb,
    x.day180_payer_moweb,
    x.day360_payer_moweb,
    x.day540_payer_moweb,
    x.day735_payer_moweb,
    x.day1_payer_desktop,
    x.day2_payer_desktop,
    x.day3_payer_desktop,
    x.day7_payer_desktop,
    x.day14_payer_desktop,
    x.day30_payer_desktop,
    x.day60_payer_desktop,
    x.day90_payer_desktop,
    x.day180_payer_desktop,
    x.day360_payer_desktop,
    x.day540_payer_desktop,
    x.day735_payer_desktop,
    x.day1_cef_app,
    x.day2_cef_app,
    x.day3_cef_app,
    x.day7_cef_app,
    x.day14_cef_app,
    x.day30_cef_app,
    x.day60_cef_app,
    x.day90_cef_app,
    x.day180_cef_app,
    x.day360_cef_app,
    x.day540_cef_app,
    x.day735_cef_app,
    x.day1_cef_moweb,
    x.day2_cef_moweb,
    x.day3_cef_moweb,
    x.day7_cef_moweb,
    x.day14_cef_moweb,
    x.day30_cef_moweb,
    x.day60_cef_moweb,
    x.day90_cef_moweb,
    x.day180_cef_moweb,
    x.day360_cef_moweb,
    x.day540_cef_moweb,
    x.day735_cef_moweb,
    x.day1_cef_desktop,
    x.day2_cef_desktop,
    x.day3_cef_desktop,
    x.day7_cef_desktop,
    x.day14_cef_desktop,
    x.day30_cef_desktop,
    x.day60_cef_desktop,
    x.day90_cef_desktop,
    x.day180_cef_desktop,
    x.day360_cef_desktop,
    x.day540_cef_desktop,
    x.day735_cef_desktop,
    x.day1_cash_entrant_app,
    x.day2_cash_entrant_app,
    x.day3_cash_entrant_app,
    x.day7_cash_entrant_app,
    x.day14_cash_entrant_app,
    x.day30_cash_entrant_app,
    x.day60_cash_entrant_app,
    x.day90_cash_entrant_app,
    x.day180_cash_entrant_app,
    x.day360_cash_entrant_app,
    x.day540_cash_entrant_app,
    x.day735_cash_entrant_app,
    x.day1_cash_entrant_moweb,
    x.day2_cash_entrant_moweb,
    x.day3_cash_entrant_moweb,
    x.day7_cash_entrant_moweb,
    x.day14_cash_entrant_moweb,
    x.day30_cash_entrant_moweb,
    x.day60_cash_entrant_moweb,
    x.day90_cash_entrant_moweb,
    x.day180_cash_entrant_moweb,
    x.day360_cash_entrant_moweb,
    x.day540_cash_entrant_moweb,
    x.day735_cash_entrant_moweb,
    x.day1_cash_entrant_desktop,
    x.day2_cash_entrant_desktop,
    x.day3_cash_entrant_desktop,
    x.day7_cash_entrant_desktop,
    x.day14_cash_entrant_desktop,
    x.day30_cash_entrant_desktop,
    x.day60_cash_entrant_desktop,
    x.day90_cash_entrant_desktop,
    x.day180_cash_entrant_desktop,
    x.day360_cash_entrant_desktop,
    x.day540_cash_entrant_desktop,
    x.day735_cash_entrant_desktop,
    coalesce(b.release_date, c.release_date, d.release_date) AS release_date,
    coalesce(b.category, c.category, d.category) as category,
    coalesce(b.cpu_speed, c.cpu_speed, d.cpu_speed) as cpu_speed,
    coalesce(b.display_resolution, c.display_resolution, d.display_resolution) as display_resolution,
    coalesce(b.display_diagonal_mm, c.display_diagonal_mm, d.display_diagonal_mm) as display_diagonal_mm,
    coalesce(b.display_diagonal_in, c.display_diagonal_in, d.display_diagonal_in) as display_diagonal_in,
    coalesce(b.model_code, c.model_code, d.model_code) as model_code,
    coalesce(b.model, c.model, d.model) as model_name,
    coalesce(b.code_name, c.code_name, d.code_name) as code_name,
    coalesce(b.oem_id, c.oem_id, d.oem_id) as oem_id,
    coalesce(b.brand, c.brand, d.brand) as brand,
    x.request_campaign_id           AS adn_campaign_id,
    x.request_adgroup               AS adn_sub_campaign_id
    FROM gsnmobile.kochava_device_summary_phoenix x
       left join temp_device_hardware_joins a
              ON a.device_original = x.device
                 AND coalesce(a.model,'null') = coalesce(x.model,'null')
       left join temp_mobile_device_data_joins b
              ON a.device_join = b.device_join
       left join temp_mobile_device_data_joins c
              ON a.device_join2 = c.device_join
       left join temp_mobile_device_data_joins d
              ON a.device_join3 = d.device_join
;


ALTER TABLE gsnmobile.kochava_device_summary_phoenix_extended, gsnmobile.kochava_device_summary_phoenix_extended_ods RENAME TO kochava_device_summary_phoenix_extended_temp, kochava_device_summary_phoenix_extended;
ALTER TABLE gsnmobile.kochava_device_summary_phoenix_extended_temp RENAME TO kochava_device_summary_phoenix_extended_ods;
TRUNCATE TABLE gsnmobile.kochava_device_summary_phoenix_extended_ods;


COMMIT;
