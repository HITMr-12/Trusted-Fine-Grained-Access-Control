import json, os, sys
from pathlib import Path
from pyspark.sql import SparkSession
r=Path('/home/lyb/fgac-lab');user=os.environ['HADOOP_USER_NAME']
spark=SparkSession.builder.appName('fgac-lab-authz-'+user).getOrCreate()
result={'user':user,'spark_user':spark.sparkContext.sparkUser(),'ok':False}
try:
    try:
        query=spark.sql('SELECT VendorID, card, amount FROM fgac.baseline.authz_fixture ORDER BY amount')
        (r/('runs/plan-'+user+'.txt')).write_text(query._jdf.queryExecution().toString())
        rows=query.collect()
    except Exception as e:
        if user!='denied':raise
        assert 'denied' in str(e).lower() or 'permission' in str(e).lower(),str(e)
        result.update(ok=True,denied=True,error=str(e)[:1200])
    else:
        assert user!='denied','Unauthorized query succeeded'
        result['rows']=[x.asDict() for x in rows]
        if user=='alice':
            assert [(x.VendorID,x.card,x.amount) for x in rows]==[(1,'MASKED',10.0),(1,None,30.0)],rows
        else:
            assert len(rows)==3 and rows[0].card=='12345678' and rows[1].card=='87654321',rows
            counts={str(x.VendorID):x['count'] for x in spark.table('fgac.nyc.taxi_trips').groupBy('VendorID').count().collect()}
            assert counts=={'1':729732,'2':2234632,'6':260},counts
            result['vendor_counts']=counts
        result['ok']=True
finally:
    (r/('runs/authz-'+user+'.json')).write_text(json.dumps(result,indent=2))
    spark.stop()
