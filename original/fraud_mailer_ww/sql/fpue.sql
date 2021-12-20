SELECT /*+ LABEL ('{label}')*/
    tournament_id AS 'Tournament Id'
    ,ttemplate_id AS 'Template Id'
    ,NAME AS 'Tournament Name'
    ,reg_open AS 'Registration Open'
    ,reg_close AS 'Registration Close'
    ,to_char(fee, '$999,990.00') as fee
    ,to_char(purse, '$999,990.00') as purse
    ,type
FROM ww.tournaments a
LEFT JOIN ww.dim_tournament_types b ON a.type = b.tournament_type
WHERE DATE(reg_close) = DATE('{today}')
    AND type = 8
    AND fee > 0
    AND purse >= 2100;
