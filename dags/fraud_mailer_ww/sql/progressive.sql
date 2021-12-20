SELECT /*+ LABEL ('{label}')*/
    tournament_id AS 'Tournament Id'
    ,ttemplate_id AS 'Template Id'
    ,NAME AS 'Tournament Name'
    ,reg_open AS 'Registration Open'
    ,reg_close AS 'Registration Close'
    ,a.prize_percent
    ,type_name
    ,to_char(sum(CASE
                   WHEN trans_type = 'SIGNUP'
                           THEN amount + play_money_amount
                   ELSE 0
                   END) * (prize_percent / 100) * - 1, '$999,990.00') AS Prize
FROM ww.internal_transactions j
INNER JOIN ww.tournaments a ON j.product_id = a.tournament_id
LEFT JOIN ww.dim_tournament_types b ON a.type = b.tournament_type
WHERE DATE (reg_close) BETWEEN (DATE('{today}') - 2)
    AND (DATE('{today}') - 1)
    AND type_name = '''Progressive'''
GROUP BY 1, 2, 3, 4, 5, 6, 7
HAVING sum(CASE
    WHEN trans_type = 'SIGNUP'
            THEN amount + play_money_amount
    ELSE 0
    END) * (prize_percent / 100) * - 1 >= 1000;
