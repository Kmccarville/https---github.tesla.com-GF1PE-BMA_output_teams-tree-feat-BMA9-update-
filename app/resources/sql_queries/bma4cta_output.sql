SELECT count(distinct tp.thingid)/28
FROM thingpath tp
WHERE
    tp.flowstepname = '3BM4-25000'
    AND tp.exitcompletioncode = 'PASS'
    AND tp.completed BETWEEN '{start_time}' AND '{end_time}'