config = {
    "Accounts": {
        "gamesnetwork":
            {
                "active": 1,
                "connection_name":"kochava_api_key_gamesnetwork",
                "api_key": "",
                "app_list": {
                    "kosolitairetripeaks179552cdd213d1b1e": ["SOLITAIRE TRIPEAKS", "android"],
                    "kosolitaire-tripeaks-ios53a8762dc3a45": ["SOLITAIRE TRIPEAKS", "ios"],
                },
                "stagetablename": "temp.ah_2105_ad_partner_installs_gamesnetwork_ods",
                "finaltablename": "temp.ah_2105_ad_partner_installs",
            },

    }
}


def get_config(gamename, gamesnetwork):
    config['Accounts'][gamename]["api_key"] = gamesnetwork
    return config['Accounts'][gamename]
