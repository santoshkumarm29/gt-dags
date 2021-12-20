SELECT /*+ LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_sftp_responsys_ww_pet_universe')*/  user_id,
       UNIVERSE_NMP,
       UNIVERSE_SPRITE,
       UNIVERSE_DRPEPPER,
       UNIVERSE_ROOTBEER,
       UNIVERSE_GINGERALE,
       UNIVERSE_COKE,
       UNIVERSE_FRESCA,
       UNIVERSE_7UP,
       UNIVERSE_FPUEONLY,
       UNIVERSE_FANTA,
       UNIVERSE_NESTEA,
       UNIVERSE_PEPSI,
       UNIVERSE_MOXIE,
       UNIVERSE_CRUSH,
       UNIVERSE_HAPPYHOUR,
       UNIVERSE_ROCKSTAR,
       UNIVERSE_PCMEMBER,
       UNIVERSE_PCSILVER,
       UNIVERSE_PCGOLD,
       UNIVERSE_PCPLATINUM,
       UNIVERSE_GSNPRACTICEPLAYERS,
       UNIVERSE_GSNCASHPLAYERS
FROM airflow.snapshot_responsys_ww_pet_universe
where ds = '{{ ds }}'
EXCEPT
SELECT user_id,
       UNIVERSE_NMP,
       UNIVERSE_SPRITE,
       UNIVERSE_DRPEPPER,
       UNIVERSE_ROOTBEER,
       UNIVERSE_GINGERALE,
       UNIVERSE_COKE,
       UNIVERSE_FRESCA,
       UNIVERSE_7UP,
       UNIVERSE_FPUEONLY,
       UNIVERSE_FANTA,
       UNIVERSE_NESTEA,
       UNIVERSE_PEPSI,
       UNIVERSE_MOXIE,
       UNIVERSE_CRUSH,
       UNIVERSE_HAPPYHOUR,
       UNIVERSE_ROCKSTAR,
       UNIVERSE_PCMEMBER,
       UNIVERSE_PCSILVER,
       UNIVERSE_PCGOLD,
       UNIVERSE_PCPLATINUM,
       UNIVERSE_GSNPRACTICEPLAYERS,
       UNIVERSE_GSNCASHPLAYERS
FROM airflow.snapshot_responsys_ww_pet_universe
where ds = '{{ yesterday_ds }}'