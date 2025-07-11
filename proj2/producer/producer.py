"""
Copyright (C) 2024 BeaconFire Staffing Solutions
Author: Ray Wang

This file is part of Oct DE Batch Kafka Project 1 Assignment.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import csv
import json
import os
from confluent_kafka import Producer
from employee import Employee
import confluent_kafka
# from pyspark.sql import SparkSession
import pandas as pd
from confluent_kafka.serialization import StringSerializer
import psycopg2
import time

EMPLOYEE_TOPIC_NAME = "bf_employee_cdc"
LAST_ID_FILE = "last_id.txt"

class cdcProducer(Producer):
    #if running outside Docker (i.e. producer is NOT in the docer-compose file): host = localhost and port = 29092
    #if running inside Docker (i.e. producer IS IN the docer-compose file), host = 'kafka' or whatever name used for the kafka container, port = 9092
    def __init__(self, host="localhost", port="29092"):
        self.host = host
        self.port = port
        producerConfig = {'bootstrap.servers':f"{self.host}:{self.port}",
                          'acks' : 'all'}
        super().__init__(producerConfig)
        self.running = True
        self.last_processed_id = self.load_last_id()
    
    def fetch_cdc(self):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5433"),
                database="postgres",
                user="postgres",
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()
            # fetch CDC rows 
            query = """
                SELECT emp_id, first_name, last_name, dob, city, salary, action
                FROM emp_cdc
                WHERE change_id > %s 
                ORDER BY change_id;
            """
            cur.execute(query, (self.last_processed_id,))
            rows = cur.fetchall()
            print("rows: " , rows)

            for row in rows:
                message = {
                    "emp_id": row[0],
                    "first_name": row[1],
                    "last_name": row[2],
                    "dob": str(row[3]),
                    "city": row[4],
                    "salary": float(row[5]),
                    "action": row[6]
                }
                self.produce(EMPLOYEE_TOPIC_NAME, json.dumps(message).encode('utf-8'))
                print(f"Sent to Kafka: {message}")
                self.last_processed_id = row[0]  # update last processed ID

            self.flush()
            self.save_last_id()  # Save it after processing all rows

            cur.close()
            conn.close()
        except Exception as err:
            print(f"Error fetching CDC: {err}")

    def load_last_id(self):
        if os.path.exists(LAST_ID_FILE):
            with open(LAST_ID_FILE, 'r') as f:
                content = f.read().strip()
                if content.isdigit():
                    return int(content)
        return 0
    
    def save_last_id(self):
        with open(LAST_ID_FILE, 'w') as f:
            f.write(str(self.last_processed_id))
    

if __name__ == '__main__':
    print("Producer starts")
    encoder = StringSerializer('utf-8')
    producer = cdcProducer(
        host=os.getenv("KAFKA_HOST", "localhost"),
        port=os.getenv("KAFKA_PORT", "29092")
    )
    
    while producer.running:
        producer.fetch_cdc()
        time.sleep(2)  # imitate a system-level cron job
    
