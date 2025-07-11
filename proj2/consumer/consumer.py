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


import os
import logging


import json
import random
import string
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer
from employee import Employee
from employee import Employee

EMPLOYEE_TOPIC_NAME = "bf_employee_cdc"

class cdcConsumer(Consumer):
    def __init__(self, group_id: str = ''): # config Kafka connection
        kafka_host = os.getenv("KAFKA_HOST", "localhost")
        kafka_port = os.getenv("KAFKA_PORT", "29092")
        self.conf = {'bootstrap.servers': f'{kafka_host}:{kafka_port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        super().__init__(self.conf)
        self.keep_running = True

    def consume(self, topics):
        try:
            self.subscribe(topics)
            while self.keep_running:
                msg = self.poll(1.0)  # Poll for messages every sec
                if msg is None:
                    continue
                if msg.error():
                    print("Consumer error:", msg.error())
                    continue

                self.update_dst(msg)
        finally:
            self.close()

    def update_dst(self, msg):
        e = json.loads(msg.value().decode('utf-8')) # deserialize with json
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                database="postgres",
                user="postgres",
                port=os.getenv("DB_PORT", "5432"),
                password="postgres")
            conn.autocommit = True
            cur = conn.cursor()

            if e['action'] == 'INSERT':
                print("Performing INSERT: ", e)
                cur.execute("""
                    INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (emp_id) DO NOTHING
                """, (e['emp_id'], e['first_name'], e['last_name'], e['dob'], e['city'], e['salary']))

            elif e['action'] == 'UPDATE':
                print("Performing UPDATE: ", e)
                cur.execute("""
                    UPDATE employees
                    SET first_name = %s, last_name = %s, dob = %s, city = %s, salary = %s
                    WHERE emp_id = %s
                """, (e['first_name'], e['last_name'], e['dob'], e['city'], e['salary'], e['emp_id']))

            elif e['action'] == 'DELETE':
                print("Performing DELETE: ", e)
                cur.execute("""
                    DELETE FROM employees WHERE emp_id = %s
                """, (e['emp_id'],))

            print(f"Applied {e['action']} for emp_id {e['emp_id']}")

            cur.close()
            conn.close()
        except Exception as err:
            print(f"DB Error: {err}")

if __name__ == '__main__':
    consumer = cdcConsumer(group_id="cdc_consumer_group")
    consumer.consume([EMPLOYEE_TOPIC_NAME])
    