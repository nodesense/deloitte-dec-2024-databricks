DataBricks cli

https://docs.databricks.com/en/dev-tools/cli/install.html


For MASK

https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-column-mask.html


Notes sharing

https://github.com/apache/spark/blob/master/docs/img/structured-streaming.pptx
 

https://notepad.pw/lT7nQSnp492O4hcP5M3u

```


https://leetquiz.com/

https://files.training.databricks.com/assessments/practice-exams/PracticeExam-DataEngineerAssociate.pdf

https://files.training.databricks.com/assessments/practice-exams/PracticeExam-DCADAS3-Python.pdf


https://www.udemy.com/course/practice-exams-databricks-certified-data-engineer-associate/?couponCode=ST19MT121224

```

order1.csv
```
order_id,amount,dept
1003,678.75,furniture
1004,123.5,laptop
```



```
spark.conf.set("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY"])
spark.conf.set("fs.s3a.secret.key", os.environ["AWS_SECRET_KEY"])
spark.conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

# Test S3 access
df = spark.read.csv("s3://gks-db-bucket/movies.csv")
df.show()
```



https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6783894296240107/2844802299536912/3024786518770907/latest.html



DataBricks Workshop code share day 2

```

import json
def delta_log(filePath):
  content = dbutils.fs.head(filePath, 20000)
  contents = content.split("\n")
  for c in contents:
    #print (c)
    c = c.strip()
    if c == "":
      continue
    parsed_json = json.loads(c.strip())
    print (json.dumps(parsed_json, indent=4))

#delta_log("dbfs:/user/hive/warehouse/deldb.db/orders/_delta_log/00000000000000000000.json")

```

```
def ls(path):
  for fileInfo in dbutils.fs.ls(path):
    print (fileInfo.path)

ls("/user/hive/warehouse/deldb.db/orders/_delta_log/")
```

```

# Reference to package install and import 

https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/6783894296240107/3215246938264315/3024786518770907/latest.html


Excercise 

https://raw.githubusercontent.com/elastic/examples/refs/heads/master/Common%20Data%20Formats/apache_logs/apache_logs

Use Python RDD, 

MEthod: GET, POST, DELETE etc

StatusCode: 200, 404, 500 etc

  Extract IP_Address, DateTime, URL, Method, StatusCode, ContentLength, UserAgent as tuple

  Try to save as CSV file

  
