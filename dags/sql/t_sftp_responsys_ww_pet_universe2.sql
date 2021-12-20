SELECT /*+ LABEL ('airflow-skill-worldwinner_gsncom_daily_imports-t_sftp_responsys_ww_pet_universe2')*/  user_id,
    UNIVERSE_GUEST,
    UNIVERSE_NMP,
    UNIVERSE_ADHOC7,
    UNIVERSE_QA,
    UNIVERSE_INTERNATIONAL,
    UNIVERSE_DOMESTIC,
    UNIVERSE_EMPLOYEESONLY,
    UNIVERSE_PREMIER,
    UNIVERSE_MODERN,
    UNIVERSE_LEGACY,
    UNIVERSE_WEBACQUIRED,
    UNIVERSE_APPACQUIRED,
    UNIVERSE_PLAYSWEBANDAPP,
    UNIVERSE_BETA,
    UNIVERSE_PAB,
    UNIVERSE_ADHOC1,
    UNIVERSE_ADHOC2,
    UNIVERSE_MIDDLE,
    UNIVERSE_ADHOC3,
    UNIVERSE_ADHOC4,
    UNIVERSE_ADHOC5,
    UNIVERSE_ADHOC6,
    UNIVERSE_LAPSEDCASH,
    UNIVERSE_PCMEMBER,
    UNIVERSE_PCSILVER,
    UNIVERSE_PCGOLD,
    UNIVERSE_PCPLATINUM,
    UNIVERSE_GSNPRACTICEPLAYERS,
    UNIVERSE_GSNCASHPLAYERS
FROM airflow.snapshot_responsys_ww_pet_universe2
where ds = '{{ ds }}'
EXCEPT
SELECT user_id,
    UNIVERSE_GUEST,
    UNIVERSE_NMP,
    UNIVERSE_ADHOC7,
    UNIVERSE_QA,
    UNIVERSE_INTERNATIONAL,
    UNIVERSE_DOMESTIC,
    UNIVERSE_EMPLOYEESONLY,
    UNIVERSE_PREMIER,
    UNIVERSE_MODERN,
    UNIVERSE_LEGACY,
    UNIVERSE_WEBACQUIRED,
    UNIVERSE_APPACQUIRED,
    UNIVERSE_PLAYSWEBANDAPP,
    UNIVERSE_BETA,
    UNIVERSE_PAB,
    UNIVERSE_ADHOC1,
    UNIVERSE_ADHOC2,
    UNIVERSE_MIDDLE,
    UNIVERSE_ADHOC3,
    UNIVERSE_ADHOC4,
    UNIVERSE_ADHOC5,
    UNIVERSE_ADHOC6,
    UNIVERSE_LAPSEDCASH,
    UNIVERSE_PCMEMBER,
    UNIVERSE_PCSILVER,
    UNIVERSE_PCGOLD,
    UNIVERSE_PCPLATINUM,
    UNIVERSE_GSNPRACTICEPLAYERS,
    UNIVERSE_GSNCASHPLAYERS
FROM airflow.snapshot_responsys_ww_pet_universe2
where ds = '{{ yesterday_ds }}';
