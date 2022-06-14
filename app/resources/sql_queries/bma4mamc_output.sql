SELECT count(distinct tp.thingid)/4
FROM thingpath tp
WHERE
    tp.flowstepname = '3BM4-34000'
    AND tp.exitcompletioncode = 'PASS'
    AND tp.completed BETWEEN '{start_time}' AND '{end_time}'