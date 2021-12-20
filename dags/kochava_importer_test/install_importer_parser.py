default_columns = {
                    'campaign_id':'attribution_campaign',
                    'campaign_name':'attribution_campaign_name',
                    'click_date': 'install_click_date_adjusted',
                    'country_code': 'installgeo_country_code',
                    'region': 'installgeo_region',
                    'device_version': 'install_device_version',
                    'device_ua': 'install_device_ua',
                    'install_date': 'install_date_adjusted',
                    'network_name': 'attribution_network_name',
                    'network_id': 'attribution_network',
                    'site_id': 'attribution_site',
                    'tracker_id': 'attribution_tracker',
                    'tracker_name': 'attribution_tracker_name',
                    'ad_partner_id': 'install_kochava_device_id',
                    'matched_on': 'install_matched_on',
                    'matched_by': 'install_matched_by',
                    }

nested_columns = {'install_original_request':
                  {
                        'request_g_id': 'g_id',
                        'request_keyword': 'keyword',
                        'request_matchtype': 'matchtype',
                        'request_network': 'network',
                        'request_cp_value1': ['cp_1', 'cp_value1'],
                        'request_cp_value2': ['cp_2', 'cp_value2'],
                        'request_cp_value3': ['cp_3', 'cp_value3'],
                        'request_cp_name1': ['cp_4', 'cp_name1'],
                        'request_cp_name2': ['cp_5', 'cp_name2'],
                        'request_cp_name3': ['cp_6', 'cp_name3'],
                        'request_bundle_id': 'cp_0',
                        'request_campaigngroup_name': 'campaign_group_name',
                        'sub_site_id': 'sub_site_id',
                        'ad_squad_id': 'ad_squad_id',
                        'ad_squad_name': 'ad_squad_name',
                        'ad_id': 'ad_id',
                        'ad_name': 'ad_name',
                        'campaign_group_id': 'campaign_group_id',
                        'campaign_group_name': 'campaign_group_name',
                  },
                  'install_devices_ids':
                  {
                    'udid': 'udid',
                    'mac_address': 'mac',
                    'idfa': 'idfa',
                    'idfv': 'idfv',
                    'adid': 'adid',
                    'android_id': 'android_id'}
}

vendor_specific_columns = {'request_campaign_id': {
    'DEFAULT': ['install_original_request.campaignid', 'install_original_request.iad-campaign-id',
                'install_original_request.campaign_id', 'install_original_request.campaign_group_id']},
    'request_adgroup': {
        'TIKTOK FOR BUSINESS - ANDROID': ['install_original_request.adgroup_name',
                                          'install_original_request.meta_data.adgroup_name'],
        'TIKTOK FOR BUSINESS - IOS': ['install_original_request.adgroup_name',
                                      'install_original_request.meta_data.adgroup_name'],
        'DEFAULT': ['install_original_request.adgroup_id', 'install_original_request.adgroup',
                    'install_original_request.iad-adgroup-id']},
    'request_adgroup_name': {
        'TIKTOK FOR BUSINESS - ANDROID': ['install_original_request.adgroup_id', 'install_original_request.adgroup',
                                          'install_original_request.iad-adgroup-id',
                                          'install_original_request.meta_data.adgroup_id'],
        'TIKTOK FOR BUSINESS - IOS': ['install_original_request.adgroup_id', 'install_original_request.adgroup',
                                      'install_original_request.iad-adgroup-id',
                                      'install_original_request.meta_data.adgroup_id'],
        'DEFAULT': ['install_original_request.adgroup_name']},
    'creative': {
        'TIKTOK FOR BUSINESS - ANDROID': ['install_original_request.creative_name',
                                          'install_original_request.meta_data.creative_name'],
        'TIKTOK FOR BUSINESS - IOS': ['install_original_request.creative_name',
                                      'install_original_request.meta_data.creative_name'],
        'DEFAULT': ['attribution_creative']
    }
}
