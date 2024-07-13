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

# Parse the CSV file
with open(file_name, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        threads.append(int(row['count']))
        p50_values.append(float(row['p50']) / 1e3)
        p95_values.append(float(row['p95']) / 1e3)
        p99_values.append(float(row['p99']) / 1e3)
        p999_values.append(float(row['p999']) / 1e3)

plt.figure(figsize=(10, 6))
plt.plot(threads, p999_values, label='p999', linestyle='solid', marker='$\u2217$')
plt.plot(threads, p99_values, label='p99', linestyle='solid', marker='$\u002B$')
plt.plot(threads, p95_values, label='p95', linestyle='solid', marker="$\u25FE$")
plt.plot(threads, p50_values, label='p50', linestyle='solid', marker="$\u25B2$")

plt.yscale("log")
plt.xlabel('Number of Threads')
plt.ylabel('Latency (µs)')
plt.grid(True)

plt.legend()

plt.tight_layout()
plt.savefig('latency_distribution.pdf')
