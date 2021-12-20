config = {'PHOENIX': {'sql/phoenix_tasks/t_validate_kochava_data.sql'                :['UPSTREAM','Phoenix data is missing in ad_partner_installs'],
                      'sql/phoenix_tasks/t_validate_nar.sql'                         :['UPSTREAM','Todays World winner data not loaded to phoenix.tournament_entry_nar'],
                      'sql/phoenix_tasks/t_validate_device_day.sql'                  :['DATA_TEAM','Phoenix Device missing in phoenix.device_day'],
                      'sql/phoenix_tasks/t_validate_devices.sql'                     :['DATA_TEAM','Phoenix Device missing in phoenix.devices'],
                      'sql/phoenix_tasks/t_validate_deposits_withdrawals.sql'        :['DATA_TEAM','Phoenix deposit data missing deposits_withdrawals']
                      },

}


def get_config(stage):
    return config[stage]
