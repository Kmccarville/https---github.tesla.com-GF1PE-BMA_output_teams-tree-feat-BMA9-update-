SELECT left(tp.flowstepname,3) as line,count(distinct tp.thingid)/4 as UPH FROM thingpath tp
WHERE tp.flowstepname in ('MC1-30000','MC2-28000') AND tp.exitcompletioncode = 'PASS'
AND tp.completed between '{start_time}' and '{end_time}'
group by tp.flowstepname