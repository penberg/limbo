#!/usr/bin/env python3

import matplotlib.pyplot as plt
import matplotlib
import csv

font = {'family' : 'normal',
        'weight' : 'bold',
        'size'   : 22}

matplotlib.rcParams.update({'font.size': 22})

file_name = 'results.csv'
threads = []
p50_values = []
p95_values = []
p99_values = []
p999_values = []

p95_limbo = []
p99_limbo = []
p999_limbo = []

# Parse the CSV file
with open(file_name, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        if row['system'] == 'rusqlite':
            threads.append(int(row['count']))
            p50_values.append(float(row['p50']) / 1e3)
            p95_values.append(float(row['p95']) / 1e3)
            p99_values.append(float(row['p99']) / 1e3)
            p999_values.append(float(row['p999']) / 1e3)
        else:
            p95_limbo.append(float(row['p95']) / 1e3)
            p99_limbo.append(float(row['p99']) / 1e3)
            p999_limbo.append(float(row['p999']) / 1e3)

plt.figure(figsize=(10, 6))
plt.plot(threads, p999_values, label='rusqlite (p999)', linestyle='solid', marker='$\u2217$')
plt.plot(threads, p999_limbo, label='limbo (p999)', linestyle='solid', marker='$\u2217$')
plt.plot(threads, p99_values, label='rusqlite (p99)', linestyle='solid', marker='$\u002B$')
plt.plot(threads, p99_limbo, label='limbo (p99)', linestyle='solid', marker='$\u002B$')
#plt.plot(threads, p95_values, label='p95', linestyle='solid', marker="$\u25FE$")
#plt.plot(threads, p50_values, label='p50', linestyle='solid', marker="$\u25B2$")

plt.yscale("log")
plt.xlabel('Number of Tenants')
plt.ylabel('Latency (µs)')
plt.grid(True)

plt.legend()

plt.tight_layout()
plt.savefig('latency_distribution.pdf')
