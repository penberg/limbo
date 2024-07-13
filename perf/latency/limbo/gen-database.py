#!/usr/bin/env python3

import argparse
import sqlite3
from faker import Faker

parser = argparse.ArgumentParser()
parser.add_argument('filename')
parser.add_argument('-c', '--count', type=int)

args = parser.parse_args()

conn = sqlite3.connect(args.filename)
cursor = conn.cursor()

# Create the user table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS user (
        id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone_number TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zipcode TEXT
    )
''')

fake = Faker()
for _ in range(args.count):
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone_number = fake.phone_number()
    address = fake.street_address()
    city = fake.city()
    state = fake.state_abbr()
    zipcode = fake.zipcode()

    cursor.execute('''
        INSERT INTO user (first_name, last_name, email, phone_number, address, city, state, zipcode)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (first_name, last_name, email, phone_number, address, city, state, zipcode))

conn.commit()
conn.close()
