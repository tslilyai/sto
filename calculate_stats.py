from collections import defaultdict
import statistics as s
import os, re, sys, json, subprocess, multiprocessing

output = defaultdict(list)
deps = []
paborts = []
speedup = []

for i in range(0,20) :
    output[i] = subprocess.check_output("./chopped_test", stderr=subprocess.STDOUT)


for result_str in output:
    result = output[result_str].decode("utf-8")
    result = result.strip().split(  )
    deps.append(float(result[0]))
    paborts.append(float(result[3]))
    speedup.append(float(result[4]))

print (s.mean(deps), s.stdev(deps))
print (s.mean(paborts), s.stdev(paborts))
print (s.mean(speedup), s.stdev(speedup))
