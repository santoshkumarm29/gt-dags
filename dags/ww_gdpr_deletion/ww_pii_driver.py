import logging
from common.hooks.vertica_hook import VerticaHook

logger = logging.getLogger('World_winner_GDPR')


def get_id_list(con, stage_name):
    with con.cursor() as cur:
        cur.execute('''Select /*+ LABEL ('airflow-worldwinner-ww_pii_gdpr-ww_pii_driver') */
                       user_id, jira_ticket_no from gdpr.worldwinner_pii_tickets
               where status = 'initiated' and {stage_name} is null;'''.format(stage_name=stage_name))
        id_list = cur.fetchall()
    return id_list


def update_phoenix_events_for_pii(vertica_conn_id='vertica_conn'):
    con = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()
    tpk_id_list = get_id_list(con, 'phoenix_events_update')
    for id in tpk_id_list:
        user_id = id[0]
        jira_ticket = id[1]
        logger.info("These are all ids : user_id = {}, jira_ticket_no = {}".format(user_id, jira_ticket))

        with con.cursor() as cur:
            cur.execute('''UPDATE /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_events_for_pii') */
                            phoenix.events_client
                            SET idfa = '00000000-0000-0000-0000-000000000000',
                                idfv = '00000000-0000-0000-0000-000000000000', 
                                mac = NULL,
                                user_name = NULL,
                                str_attr1 = case when event_name = 'registerBegin' then NULL else str_attr1 end,
                                str_attr2 = case when event_name = 'PushNotificationDebug' then NULL 
                                              event_name = 'userLoadFromPartnerLink' and key_str_attr2 = 'idfv' then NULL 
                                              else str_attr2 end
                            WHERE
                                user_id ='{user_id}';'''.format(user_id=user_id))

            cur.execute('''UPDATE /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_events_for_pii') */
                            phoenix.events_solrush_client
                            SET idfa = '00000000-0000-0000-0000-000000000000',
                                idfv = '00000000-0000-0000-0000-000000000000', 
                                mac = NULL,
                                user_name = NULL,
                                str_attr1 = case when event_name = 'registerBegin' then NULL else str_attr1 end,
                                str_attr2 = case when event_name = 'PushNotificationDebug' then NULL 
                                              event_name = 'userLoadFromPartnerLink' and key_str_attr2 = 'idfv' then NULL 
                                              else str_attr2 end
                            WHERE
                                user_id ='{user_id}';'''.format(user_id=user_id))

            cur.execute('''UPDATE /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_events_for_pii') */
                            phoenix.events_web
                            SET 
                                user_name = NULL
                            WHERE
                                event_name = 'registrationSuccessful'
                                and user_id ='{user_id}';'''.format(user_id=user_id))

            cur.execute("COMMIT")
            logger.info("phoenix events update success")
            update_status_to_log(con, 'phoenix_events_update', 'user_id', 'completed', user_id)

    con.commit()
    logger.info("Done!")


def update_phoenix_ds_tables_for_pii(vertica_conn_id='vertica_conn'):
    con = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()
    tpk_id_list = get_id_list(con, 'phoenix_ds_tables_update')
    for id in tpk_id_list:
        user_id = id[0]
        jira_ticket = id[1]
        logger.info("These are all ids : user_id = {}, jira_ticket_no = {}".format(user_id, jira_ticket))

        with con.cursor() as cur:
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.deposits_withdrawals WHERE user_id ='{user_id}';'''.format(user_id=user_id))
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.device_day 
                           WHERE idfv in (SELECT idfv FROM phoenix.dim_device_mapping 
                                           WHERE id_type = 'user_id' AND id = '{user_id}';'''.format(user_id=user_id))
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.device_sessions 
                           WHERE idfv in (SELECT idfv FROM phoenix.dim_device_mapping 
                                           WHERE id_type = 'user_id' AND id = '{user_id}';'''.format(user_id=user_id))
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.devices 
                           WHERE idfv in (SELECT idfv FROM phoenix.dim_device_mapping 
                                           WHERE id_type = 'user_id' AND id = '{user_id}';'''.format(user_id=user_id))
            cur.execute('''Update /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.tournament_cef  
                           set idfv = '00000000-0000-0000-0000-000000000000'
                           where user_id = '{user_id}';'''.format(user_id=user_id))
            cur.execute('''Update /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.tournament_entry_nar  
                           set idfv = '00000000-0000-0000-0000-000000000000'
                           where user_id = '{user_id}';'''.format(user_id=user_id))
            # solrush tables
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */ 
                           phoenix.solrush_deposits_withdrawals WHERE user_id ='{user_id}';'''.format(user_id=user_id))
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.solrush_device_day 
                           WHERE idfv in (SELECT idfv FROM phoenix.dim_device_mapping 
                                           WHERE id_type = 'user_id' AND id = '{user_id}';'''.format(user_id=user_id))
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.solrush_device_sessions 
                           WHERE idfv in (SELECT idfv FROM phoenix.dim_device_mapping 
                                           WHERE id_type = 'user_id' AND id = '{user_id}';'''.format(user_id=user_id))
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.solrush_devices 
                           WHERE idfv in (SELECT idfv FROM phoenix.dim_device_mapping 
                                           WHERE id_type = 'user_id' AND id = '{user_id}';'''.format(user_id=user_id))
            cur.execute('''Update /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.dim_user_idfv_first_event_solrush  
                           set idfv = '00000000-0000-0000-0000-000000000000'
                           where user_id = '{user_id}';'''.format(user_id=user_id))
            cur.execute('''Update /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.solrush_tournament_cef  
                           set idfv = '00000000-0000-0000-0000-000000000000'
                           where user_id = '{user_id}';'''.format(user_id=user_id))
            cur.execute('''Update /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_phoenix_ds_tables_for_pii') */
                           phoenix.solrush_tournament_entry_nar  
                           set idfv = '00000000-0000-0000-0000-000000000000'
                           where user_id = '{user_id}';'''.format(user_id=user_id))
            cur.execute("COMMIT")
            logger.info("Phoenix downstream tables update success")
            update_status_to_log(con, 'phoenix_ds_tables_update', 'user_id', 'completed', user_id)

    con.commit()
    logger.info("Done!")


def update_ww_schema_for_pii(vertica_conn_id='vertica_conn'):
    con = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()
    tpk_id_list = get_id_list(con, 'ww_schema_update')
    for id in tpk_id_list:
        user_id = id[0]
        jira_ticket = id[1]
        logger.info("These are all ids : user_id = {}, jira_ticket_no = {}".format(user_id, jira_ticket))

        with con.cursor() as cur:
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_ww_schema_for_pii') */
                           from ww.dim_profiles_private WHERE user_id ='{user_id}';'''.format(user_id=user_id))
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_ww_schema_for_pii') */
                           from ww.device_user_agents WHERE user_id ='{user_id}';'''.format(user_id=user_id))
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_ww_schema_for_pii') */
                           from ww.platform_stats WHERE user_id ='{user_id}';'''.format(user_id=user_id))
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_ww_schema_for_pii') */
                           from ww.platform_stats_agg WHERE user_id ='{user_id}';'''.format(user_id=user_id))
            cur.execute('''UPDATE /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_ww_schema_for_pii') */
                           ww.dim_user_aux SET display_name = NULL, avatar_id = NULL, avatar_v2_id = NULL 
                           WHERE user_id ='{user_id}';'''.format(user_id=user_id))
            cur.execute("COMMIT")
            logger.info("ww schema update success")
            update_status_to_log(con, 'ww_schema_update', 'user_id', 'completed', user_id)

    con.commit()
    logger.info("Done!")


def update_gsnmobile_schema_for_pii(vertica_conn_id='vertica_conn'):
    con = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()
    tpk_id_list = get_id_list(con, 'gsnmobile_schema_update')
    for id in tpk_id_list:
        user_id = id[0]
        jira_ticket = id[1]
        logger.info("These are all ids : user_id = {}, jira_ticket_no = {}".format(user_id, jira_ticket))

        with con.cursor() as cur:
            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_gsnmobile_schema_for_pii') */
                              from gsnmobile.kochava_device_summary_phoenix 
                              WHERE idfv IN (SELECT idfv FROM phoenix.dim_device_mapping 
                              WHERE id_type = 'user_id' and id ='{user_id}';'''.format(user_id=user_id))

            cur.execute('''Delete /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_gsnmobile_schema_for_pii') */
                              from gsnmobile.kochava_device_summary_phoenix_extended 
                              WHERE idfv IN (SELECT idfv FROM phoenix.dim_device_mapping 
                              WHERE id_type = 'user_id' and id ='{user_id}';'''.format(user_id=user_id))

            cur.execute('''update /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_gsnmobile_schema_for_pii') */
                                       gsnmobile.ad_partner_installs
                                       set idfv = NULL,
                                           idfa = NULL,
                                           mac_address = '00:00:00:00:00:00',
                                           android_id = NULL,
                                           device_id = NULL
                                       WHERE app_name in ('WW SOLRUSH','PHOENIX') 
                                          and case when platform = 'ios' then idfv else android_id end IN (SELECT idfv FROM phoenix.dim_device_mapping 
                                          WHERE id_type = 'user_id' 
                                          and id ='{user_id}';'''.format(user_id=user_id))

            cur.execute('''update /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_gsnmobile_schema_for_pii') */
                                                   gsnmobile.kochava_unattributed_installs
                                                   set idfv = NULL,
                                                       idfa = NULL,
                                                       mac_address = '00:00:00:00:00:00',
                                                       android_id = NULL,
                                                       device_id = NULL
                                                   WHERE app_name in ('WW SOLRUSH','PHOENIX') 
                                                      and case when platform = 'ios' then idfv else android_id end IN (SELECT idfv FROM phoenix.dim_device_mapping 
                                                      WHERE id_type = 'user_id' 
                                                      and id ='{user_id}';'''.format(user_id=user_id))

            # idfv always null - we can ignore below update
            # cur.execute('''update /*+ /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_gsnmobile_schema_for_pii') */
            #                            gsnmobile.ad_partner_clicks
            #                            set idfv = NULL,
            #                                idfa = NULL,
            #                                origination_ip = '0.0.0.0',
            #                                android_id = NULL,
            #                                id = NULL
            #                            WHERE app_name in ('WW SOLRUSH','PHOENIX')
            #                               and idfv IN (SELECT idfv FROM phoenix.dim_device_mapping
            #                               WHERE id_type = 'user_id' and id ='{user_id}';'''.format(user_id=user_id))

            # using synthetic_id
            cur.execute('''update /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_gsnmobile_schema_for_pii') */
                                       gsnmobile.ad_partner_notifications
                                       set device_id = NULL
                                       WHERE app_id = 'PHOENIX' and synthetic_id IN (SELECT synthetic_id FROM phoenix.dim_device_mapping
                                          WHERE id_type = 'synthetic_id' and idfv in 
                                          (Select idfv and from phoenix.dim_device_mapping 
                                           where id_type = 'user_id' and id ='{user_id}'));'''.format(user_id=user_id))

            # Any idea how to filter the below for phoenix?
            cur.execute('''DELETE /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_gsnmobile_schema_for_pii') */
                           FROM gsnmobile.dim_device_mapping 
                           WHERE synthetic_id IN (SELECT synthetic_id FROM phoenix.dim_device_mapping
                                          WHERE id_type = 'synthetic_id' and idfv in 
                                          (Select idfv and from phoenix.dim_device_mapping 
                                           where id_type = 'user_id' and id ='{user_id}'));'''.format(user_id=user_id))

            cur.execute('''Update /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_gsnmobile_schema_for_pii') */
                           gsnmobile.worldwinner_mobile_ltv_predictions_historical 
                           set subject_id = '00000000-0000-0000-0000-000000000000'
                           WHERE subject_id in (SELECT idfv FROM phoenix.dim_device_mapping 
                                           WHERE id_type = 'user_id' AND id = '{user_id}';'''.format(user_id=user_id))

            cur.execute("COMMIT;")
            logger.info("gsnmobile schema update success")
            update_status_to_log(con, 'gsnmobile_schema_update', 'user_id', 'completed', user_id)

    con.commit()
    logger.info("Done!")


def update_dim_device_map_for_pii(vertica_conn_id='vertica_conn'):
    con = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()
    tpk_id_list = get_id_list(con, 'dim_device_map_update')
    for id in tpk_id_list:
        user_id = id[0]
        jira_ticket = id[1]
        logger.info("These are all ids : user_id = {}, jira_ticket_no = {}".format(user_id, jira_ticket))

        with con.cursor() as cur:
            cur.execute('''DELETE /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_dim_device_map_for_pii') */
                           FROM phoenix.dim_device_mapping_raw
                           WHERE idfv IN (SELECT idfv FROM phoenix.dim_device_mapping 
                                          WHERE id_type = 'user_id' 
                                          and id ='{user_id}';'''.format(user_id=user_id))

            cur.execute('''DELETE /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_update_dim_device_map_for_pii') */
                           FROM phoenix.dim_device_mapping 
                           WHERE idfv IN (SELECT idfv FROM phoenix.dim_device_mapping 
                                          WHERE id_type = 'user_id' 
                                          and id ='{user_id}';'''.format(user_id=user_id))

            cur.execute("COMMIT")
            logger.info("dim device mapping delete success")
            update_status_to_log(con, 'dim_device_map_update', 'user_id', 'completed', user_id)

    con.commit()
    logger.info("Done!")


def update_status_to_log(con, status_column_name, id_column_name, status, id):
    with con.cursor() as cur:
        update_stmt = '''update /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-ww_pii_driver') */
                         gdpr.worldwinner_pii_tickets set {status_column_name} = '{status}'
                         where {id_column_name} = '{id}' '''.format(status_column_name=status_column_name,
                                                                    status=status, id_column_name=id_column_name, id=id)
        print(update_stmt)
        cur.execute(update_stmt)
        cur.execute('COMMIT;')
    con.commit()

def update_ticket_status(vertica_conn_id='vertica_conn'):
    con = VerticaHook(vertica_conn_id=vertica_conn_id, resourcepool='default').get_conn()

    logger.info('final ticket level update')
    with con.cursor() as cur:
        cur.execute('''Update /*+ direct, LABEL ('airflow-worldwinner-ww_pii_gdpr-t_final_ticket_update') */
                       gdpr.worldwinner_pii_tickets set status = case when phoenix_events_update = 'completed'
                                                                        and phoenix_ds_tables_update = 'completed'
                                                                        and ww_schema_update = 'completed'
                                                                        and gsnmobile_schema_update = 'completed'
                                                                        and dim_device_map_update = 'completed'
                                                                        then 'success'
                                                                        else 'failed'
                                                                    end,
                                                                 date_of_completion = case
                                                                        when phoenix_events_update = 'completed'
                                                                        and phoenix_ds_tables_update = 'completed'
                                                                        and ww_schema_update = 'completed'
                                                                        and gsnmobile_schema_update = 'completed'
                                                                        and dim_device_map_update = 'completed'
                                                                        then current_timestamp
                                                                        else NULL
                                                                    end
                                                                    where status = 'initiated'; ''')
        cur.execute("COMMIT;")
        logger.info('update status done')

    con.commit()
    logger.info("Done!")
