import json
from pathlib import Path
from pyspark.sql import SparkSession
r=Path('/home/lyb/fgac-lab');s=SparkSession.builder.appName('polaris-metadata-validation').getOrCreate();result={'ok':False}
try:
    counts={str(x.VendorID):x['count'] for x in s.table('fgac.nyc.taxi_trips').groupBy('VendorID').count().collect()}
    assert counts=={'1':729732,'2':2234632,'6':260},counts
    result['vendor_counts']=counts
    name='fgac.baseline.polaris_write_probe'
    assert not s.catalog.tableExists(name),'Probe already exists; refusing to overwrite'
    s.sql('CREATE TABLE '+name+' (id INT, value STRING) USING iceberg')
    s.sql("INSERT INTO "+name+" VALUES (1, 'polaris-managed')")
    row=s.sql('SELECT * FROM '+name).collect()
    assert len(row)==1 and row[0].value=='polaris-managed',row
    result['create_insert_read']=True
    s.sql('DROP TABLE '+name+' PURGE')
    assert not s.catalog.tableExists(name)
    result.update(drop=True,ok=True)
finally:
    (r/'runs/polaris-metadata-validation.json').write_text(json.dumps(result,indent=2));s.stop()
