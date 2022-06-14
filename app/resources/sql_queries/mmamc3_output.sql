    SELECT count(distinct tp.thingid)/4 FROM thingpath tp
    WHERE tp.flowstepname = 'MBM-25000' AND tp.exitcompletioncode = 'PASS' AND tp.completed between '{start}' and '{end}'