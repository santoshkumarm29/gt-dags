config = {
    "Accounts": {
        "gamesnetwork":
            {
                "active": 1,
                "connection_name":"kochava_api_key_gamesnetwork",
                "api_key": "",
                "app_list": {
                    "kogsn-phoenix-android-vjuqa": ["PHOENIX", "android"],
                    "kogsn-phoenix-ios-xkv": ["PHOENIX", "ios"]
                },
                "stagetablename": "gsnmobile.ad_partner_installs_gamesnetwork_ods",
                "finaltablename": "gsnmobile.ad_partner_installs",
                "traffic_including": "unattributed_traffic"
            }
    }
}

def get_config(gamename, gamesnetwork):
    config['Accounts'][gamename]["api_key"] = gamesnetwork
    return config['Accounts'][gamename]