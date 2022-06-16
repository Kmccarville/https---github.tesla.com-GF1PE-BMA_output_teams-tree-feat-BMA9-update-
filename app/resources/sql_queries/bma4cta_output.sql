SELECT
a.name,
round(COUNT(distinct tp.thingid)/28, 2) as UPH
FROM thingpath tp
JOIN actor a on tp.modifiedby = a.id
WHERE tp.flowstepname = '3BM4-25000'
AND tp.exitcompletioncode = 'PASS'
AND tp.completed BETWEEN '{start_time}' AND '{end_time}'
GROUP BY 1
ORDER BY a.name ASC;