from kafka import KafkaProducer
from kafka.errors import KafkaError
import random
import uuid
import datetime
import json
import time

#  kafka-topics  --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4 --topic invoices2

# for testing to check whether we have invoice generated or not.
# kafka-console-consumer --bootstrap-server localhost:9092 --topic invoices2


TOPIC = "invoices2"
SAMPLES = 50000
DELAY = 5 # seconds

countries = ["USA", "CA", "IN", "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"]
stock_codes = ['85123A', '71053', '84406B', '84406G', '84406E']
customer_codes = [17850, 13047, 12583, 17850]

# broker runs in linux machine
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


for i in range(SAMPLES):
    country =  random.choice(countries)
    invoice_no = str(uuid.uuid4().fields[-1])[:6]
    invoice_no = int(invoice_no)
    
    customer_code =  random.choice(customer_codes)
   
    current_time = datetime.datetime.now()
 
    invoice_date = current_time.strftime('%m/%d/%Y %H:%M') 
    
    number_of_items = random.randint(3, 10)
    
    for j in range(number_of_items):
        # MM/dd/yyyy hh:mm
        quantity = random.randint(1, 10)
        unit_price = float(random.randint(1, 5))
        stock_code =  random.choice(stock_codes)
        invoice = {  "InvoiceNo": invoice_no,
                     "StockCode": stock_code ,
                     "Quantity": quantity,
                    "Description": "TODO",
                    "InvoiceDate": invoice_date,
                    "UnitPrice": unit_price,
                    "CustomerID": customer_code,
                     "Country"  : country   }
    
        invoice_str = json.dumps(invoice)
        print ("POS ", invoice_str)

        key = invoice["Country"]
        producer.send(TOPIC, key=bytes(key,'utf-8'), value=bytes(invoice_str, 'utf-8'))
        
    print('-' * 50)
    time.sleep(DELAY)