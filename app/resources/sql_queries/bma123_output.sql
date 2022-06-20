SELECT
    t3.flowstepname AS FlowStep,
    t4.name AS Thingname,
    t6.name AS ActorModifiedby,
    (CASE
        WHEN t6.name = '3BM1-29500-01' THEN 'LINE1'
        WHEN t6.name = '3BM2-29500-01' THEN 'LINE2'
        WHEN t6.name = '3BM3-29500-01' THEN 'LINE3'
        WHEN t6.name = '3BM1-40001-01' THEN 'LINE1'
        WHEN t6.name = '3BM2-40001-01' THEN 'LINE2'
        WHEN t6.name = '3BM3-40001-01' THEN 'LINE3'
        ELSE NULL
    END) AS Line_number,
    t9.name AS CreatedBy,
    mid(t9.name, 17, 5)AS tag_number,
    (CASE
        WHEN t9.name = 'ignition-gf1-bm-tag7-prod' THEN 'CTA1'
        WHEN t9.name = 'ignition-gf1-bm-tag8-prod' THEN 'CTA2'
        WHEN t9.name = 'ignition-gf1-bm-tag9-prod' THEN 'CTA3'
        ELSE NULL
    END) AS Created_By,
    t1.name AS Startedby,
    CONVERT_TZ(t3.completed, 'GMT', 'US/Pacific'),
    t2.partnumber AS PartNumber,
    t4.created
FROM
    sparq.thingpath t3
        LEFT OUTER JOIN
    sparq.actor t1 ON (t3.modifiedby = t1.id)
        LEFT OUTER JOIN
    sparq.actor t6 ON (t3.actormodifiedby = t6.id)
        LEFT OUTER JOIN
    sparq.actor t7 ON (t3.actorcreatedby = t7.id)
        LEFT OUTER JOIN
    sparq.actor t8 ON (t3.modifiedby = t8.id)
        LEFT OUTER JOIN
    sparq.actor t9 ON (t3.createdby = t9.id)
        LEFT OUTER JOIN
    sparq.thing t4 ON (t3.thingid = t4.id)
        LEFT OUTER JOIN
    sparq.part t2 ON (t4.partid = t2.id)
WHERE
    t3.flowstepname IN ('3BM-29500', '3BM-20000', '3BM-40001')
        AND t3.completed between '{start_time}' and '{end_time}'
