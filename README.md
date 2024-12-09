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

  
