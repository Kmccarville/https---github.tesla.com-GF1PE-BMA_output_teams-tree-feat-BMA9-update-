SELECT count(distinct tp.thingid)/4 FROM thingpath tp
WHERE tp.flowstepname = '3BM5-45000' AND tp.exitcompletioncode = 'PASS' AND tp.completed between '{start_time}' and '{end_time}'
