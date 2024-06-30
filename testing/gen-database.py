#!/usr/bin/env python3

import sqlite3
from faker import Faker
import random

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

# Create the user table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        phone_number TEXT,
        address TEXT,
        city TEXT,
        state TEXT,
        zipcode TEXT,
        age INTEGER
    )
''')

fake = Faker()
for _ in range(10000):
    first_name = fake.first_name()
    last_name = fake.last_name()
    email = fake.email()
    phone_number = fake.phone_number()
    address = fake.street_address()
    city = fake.city()
    state = fake.state_abbr()
    zipcode = fake.zipcode()
    age = random.randint(0, 100) % 99

    cursor.execute('''
        INSERT INTO users (first_name, last_name, email, phone_number, address, city, state, zipcode, age)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (first_name, last_name, email, phone_number, address, city, state, zipcode, age))



conn.commit()
conn.close()
