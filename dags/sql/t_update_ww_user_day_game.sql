DELETE/*+ direct,LABEL ('airflow-skill-etl_ww_user_day-t_update_ww_user_day_game')*/
FROM ww.user_day_game
WHERE event_day = date('{{ds}}');

INSERT /*+ direct,LABEL ('airflow-skill-etl_ww_user_day-t_update_ww_user_day_game')*/INTO ww.user_day_game
SELECT
  user_id,
  event_day,
  game,
  SUM(CASE
    WHEN platform = 'desktop' THEN ump
  END) AS ump_desktop,
  SUM(CASE
    WHEN platform = 'moweb' THEN ump
  END) AS ump_moweb,
  SUM(CASE
    WHEN platform = 'phx' THEN ump
  END) AS ump_phx,
  SUM(ump) AS ump_total,
  SUM(CASE
    WHEN platform = 'desktop' THEN cef
  END) AS cef_desktop,
  SUM(CASE
    WHEN platform = 'moweb' THEN cef
  END) AS cef_moweb,
  SUM(CASE
    WHEN platform = 'phx' THEN cef
  END) AS cef_phx,
  SUM(cef) AS cef_total,
  SUM(CASE
    WHEN platform = 'desktop' THEN cgp
  END) AS cgp_desktop,
  SUM(CASE
    WHEN platform = 'moweb' THEN cgp
  END) AS cgp_moweb,
  SUM(CASE
    WHEN platform = 'phx' THEN cgp
  END) AS cgp_phx,
  SUM(cgp) AS cgp_total,
  ROUND(SUM(CASE
    WHEN platform = 'desktop' THEN cef
  END) - SUM(winnings) * SUM(CASE
    WHEN platform = 'desktop' THEN cef
  END) / Nullifzero(SUM(cef)) ::
  FLOAT, 2)
  AS nar_desktop,
  ROUND(SUM(CASE
    WHEN platform = 'moweb' THEN cef
  END) - SUM(winnings) * SUM(CASE
    WHEN platform = 'moweb' THEN cef
  END) / Nullifzero(SUM(cef)) ::
  FLOAT, 2)
  AS nar_moweb,
  ROUND(SUM(CASE
    WHEN platform = 'phx' THEN cef
  END) - SUM(winnings) * SUM(CASE
    WHEN platform = 'phx' THEN cef
  END) / Nullifzero(SUM(cef)) ::
  FLOAT, 2)
  AS nar_phx,
  ROUND((SUM(cef) - SUM(winnings)) ::FLOAT, 2) AS nar_total,
  SUM(tef) AS tef,
  SUM(fgp) AS fgp,
  SUM(fgp) + SUM(cgp) AS tgp,
  SUM(pef) AS pef,
  SUM(winnings) AS winnings
FROM (SELECT
  user_id,
  trans_date ::DATE AS event_day,
  COALESCE(name, Regexp_substr(a.description,
  'Ladder Joining Fee: (.*) Ladder', 1, 1, 'c', 1)) AS game,
  CASE
    WHEN device IS NULL THEN 'desktop'
    WHEN platform = 'skill-app' THEN 'phx'
    WHEN (platform NOT IN ('skill-app', 'skill-solrush') OR
      platform IS NULL) AND
      COALESCE(name, Regexp_substr(a.description,
      'Ladder Joining Fee: (.*) Ladder', 1, 1, 'c', 1)) IN (
      'Pyramid Solitaire', 'Bejeweled 2',
      'Bejeweled Blitz: Rubies and Riches',
      'Dynomite',
      'Cubis', 'Luxor', 'Mahjongg Dimensions',
      'Pyramid Solitaire',
      'SCRABBLE BOGGLE', 'Solitaire TriPeaks',
      'Spades', 'Spider Solitaire',
      'SwapIt!', 'Swipe Hype', 'Tetris&reg; Burst',
      'TRIVIAL PURSUIT',
      'Vegas Nights 2', 'Word Mojo',
      'Angry Birds Champions') THEN 'desktop'
    WHEN (platform NOT IN ('skill-app', 'skill-solrush') OR
      platform IS NULL) THEN 'moweb'
  END AS platform,
  MAX(1) AS ump,
  SUM(amount * -1) AS cef,
  COUNT(int_trans_id) AS cgp,
  0 AS pef,
  0 AS tef,
  0 AS winnings,
  0 AS fgp
FROM ww.internal_transactions a
LEFT JOIN ww.clid_combos_ext b
  ON a.client_id = b.client_id
LEFT JOIN ww.tournament_games c
  ON a.product_id = c.tournament_id
LEFT JOIN ww.dim_gametypes d
  ON c.gametype_id = d.gametype_id
WHERE trans_date ::DATE = date('{{ ds }}')
AND trans_type = 'SIGNUP'
AND COALESCE(amount, 0) <> 0
GROUP BY 1,
         2,
         3,
         4
UNION
SELECT
  user_id,
  trans_date ::DATE AS event_day,
  COALESCE(name, Regexp_substr(a.description,
  'Ladder Joining Fee: (.*) Ladder', 1, 1, 'c', 1)) AS game,
  NULL AS platform,
  0 AS ump,
  0 AS cef,
  0 AS cgp,
  SUM(CASE
    WHEN trans_type = 'SIGNUP' THEN play_money_amount * -1
    ELSE 0
  END) AS pef,
  SUM(CASE
    WHEN trans_type = 'SIGNUP' THEN amount + play_money_amount
    ELSE 0
  END) * -1 AS tef,
  SUM(CASE
    WHEN trans_type = 'WINNINGS' THEN amount
    ELSE 0
  END) AS winnings,
  COUNT(CASE
    WHEN trans_type = 'SIGNUP' AND
      COALESCE(play_money_amount, 0) <> 0 THEN int_trans_id
  END) AS fgp
FROM ww.internal_transactions a
LEFT JOIN ww.clid_combos_ext b
  ON a.client_id = b.client_id
LEFT JOIN ww.tournament_games c
  ON a.product_id = c.tournament_id
LEFT JOIN ww.dim_gametypes d
  ON c.gametype_id = d.gametype_id
WHERE trans_date ::DATE = date('{{ ds }}')
GROUP BY 1,
         2,
         3) a
WHERE game IS NOT NULL
GROUP BY 1,
         2,
         3
ORDER BY event_day, user_id;

COMMIT;

--SELECT PURGE_TABLE('ww.user_day_game');

