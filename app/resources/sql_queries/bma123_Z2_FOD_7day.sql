SELECT 
distinct left(a.name,4) as 'MAMC Actor', nc.thingname, nc.state as 'NC State', nc.createdby as 'NC CreatedBy', nc.detectedatstep as 'FOD caught at', nc.modified as 'NC modified', nca.disposition as 'NC Disposition', t.state as 'Module State', t.created as thingCreated
FROM
   nc force index (ix_nc_processname_created)
   inner join thing t
   on t.id = nc.thingid
   inner join actor a
   on a.id = t.actorcreatedby
   left join ncaction nca on nca.ncid = nc.id
 WHERE
  nc.symptom = 'COSMETIC/DAMAGE'
  AND nc.subsymptom = 'CONTAMINATION/ DEBRIS'
  AND nc.processname = '3BM-Module'
  AND nc.created >= NOW() - INTERVAL 7 day
  AND nc.description not like '%%max pull test%%'
  AND (nc.description like '%%foreign%%' or nc.description like '%%fiber%%' or nc.description like '%%tape%%' or nc.description like '%%adhesive%%' or nc.description like '%%glove%%')
	
            
