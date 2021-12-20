config = {
    "stagetablename": "gsnmobile.ad_partner_impressions_ods",
    "finaltablename": "gsnmobile.ad_partner_impressions",
    "Accounts": {
        "gamesnetwork":
            {
                "active": 1,
                "api_ke`y": "",
                "app_list": {
                    "kogsncasinoios1084fd911cc8abda": ["GSN CASINO", "ios"],
                    "kogsncasinoandroid1074fd911b532c1f": ["GSN CASINO", "android"],
                    "kogsncasinoamazon171052b0d49171646": ["GSN CASINO", "amazon"],
                    "kosolitairetripeaks179552cdd213d1b1e": ["SOLITAIRE TRIPEAKS", "android"],
                    "kosolitaire-tripeaks-ios53a8762dc3a45": ["SOLITAIRE TRIPEAKS", "ios"],
                    "kosolitaire-tripeaks-amazon542c346dd4e8d": ["SOLITAIRE TRIPEAKS", "amazon"],
                    "kogsn-grand-casino5435795c319a2": ["GSN GRAND CASINO", "android"],
                    "kogsn-grand-casino-ios54908ab34428c": ["GSN GRAND CASINO", "ios"],
                    "kogsn-grand-casino-amazon55f0bf591a614": ["GSN GRAND CASINO", "amazon"],
                    "kowheel-of-fortune-slots-ios56f2e2db2b2a7": ["WOF SLOTS", "ios"],
                    "kowheel-of-fortune-slots-android56f2e433835ea": ["WOF SLOTS", "android"],
                    "kowheel-of-fortune-slots-amazon56f2e46ab46d1": ["WOF SLOTS", "amazon"],
                    "kogsn-phoenix-android-vjuqa": ["PHOENIX", "android"],
                    "kogsn-phoenix-ios-xkv": ["PHOENIX", "ios"],
                    # "koworldwinner-mobile-solitaire-j4e": ["WW SOLRUSH", "ios"],
                    # "kosmash-ios-wp7": ["SMASH", "ios"],
                    # "kosmash-android-a5es": ["SMASH", "android"],
                    # "kosmash-amazon-wi31dm": ["SMASH", "amazon"],
                    "komafia-world-amazon-release-vxygnm5": ["MAFIA WORLD", "amazon"],
                    "komafia-world-android-release-wql3": ["MAFIA WORLD", "android"],
                    "komafia-world-ios-release-5jw2": ["MAFIA WORLD", "ios"],
                }

            },
        "bitrhymes":
            {
                "active": 1,
                "api_key": "",
                "app_list": {
                    "komybingobash9975204842daa9f9": ["BINGO BASH", "amazon"],
                    "kobingobashiosbitrhymes417510886c6bf32b": ["BINGO BASH", "ios"],
                    "kobingobashhdiosbitrhymes41651088666e779a": ["BINGO BASH", "ios"],
                    "kobingobashandroidbitrhymes41851088983018ae": ["BINGO BASH", "android"],
                    #"koslotsbashkindle48405397ffcb0c29f": ["SLOTS BASH", "amazon"],
                    #"koslotsbashiphone15995295906403688": ["SLOTS BASH", "ios"],
                    #"koslotsbash77351b7a4a7295c7": ["SLOTS BASH", "ios"],
                    #"koslotsbashandroid15745293579946bfc": ["SLOTS BASH", "android"]
                }
            },
        "idlegames":
            {
                "active": 0,
                "api_key": "",
                "app_list": {
                    "kofreshdeckpokeriosbyidlegames264752febb672dab9": ["FRESH DECK POKER", "ios"],
                    "kofreshdeckpokerandroidbyidlegames265052febcbaaee77": ["FRESH DECK POKER", "android"],
                    "kofreshdeckpokerkindlebyidlegames265152fec07e6e49b": ["FRESH DECK POKER", "amazon"],
                    "kogo-poker-ios-by-idle-games53d1663b155f0": ["GO POKER", "ios"],
                    "kogo-poker-android-by-idle-games53d1696eae3c4": ["GO POKER", "android"],
                    "kogo-poker-kindle-by-idle-games53d16c3543ef5": ["GO POKER", "amazon"],
                    "koslotsoffuniosbyidlegames264852febbc612eba": ["SLOTS OF FUN", "ios"],
                    "koslotsoffunandroidbyidlegames264952febc65de7f1": ["SLOTS OF FUN", "android"],
                    "koslotsoffunkindlebyidlegames4232536bfa364a78c": ["SLOTS OF FUN", "amazon"]

                }

            }

    }
}


def get_config(gamesnetwork, bitrhymes, idlegames):
    config['Accounts']['gamesnetwork']["api_key"] = gamesnetwork
    config['Accounts']['bitrhymes']["api_key"] = bitrhymes
    config['Accounts']['idlegames']["api_key"] = idlegames
    return config
