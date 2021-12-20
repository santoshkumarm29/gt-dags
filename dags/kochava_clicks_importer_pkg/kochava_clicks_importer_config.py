config = {
    "stagetablename": "gsnmobile.ad_partner_clicks_ods",
    "finaltablename": "gsnmobile.ad_partner_clicks",
    "Accounts": {
        "gamesnetwork":
            {
                "active": 1,
                "api_key": "",
                "app_list": {
                    "kogsn-phoenix-android-vjuqa": ["PHOENIX", "android"],
                    "kogsn-phoenix-ios-xkv": ["PHOENIX", "ios"],
                    "koworldwinner-mobile-solitaire-j4e": ["WW SOLRUSH", "ios"],
                }
            }
    }
}

def get_config(gamesnetwork):
    config['Accounts']['gamesnetwork']["api_key"] = gamesnetwork
    return config