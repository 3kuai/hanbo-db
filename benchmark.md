##env
centerOS-7

##before fastUtil:

    $ redis-benchmark -p 16379 -t set  -n 10000 -r 100000 -c 50
    ====== SET ======
      10000 requests completed in 1.15 seconds
      50 parallel clients
      3 bytes payload
      keep alive: 1
    
    0.01% <= 1 milliseconds
    0.06% <= 2 milliseconds
    0.49% <= 3 milliseconds
    1.63% <= 4 milliseconds
    39.76% <= 5 milliseconds
    51.34% <= 6 milliseconds
    97.10% <= 7 milliseconds
    98.12% <= 8 milliseconds
    98.43% <= 9 milliseconds
    99.30% <= 10 milliseconds
    99.41% <= 11 milliseconds
    99.56% <= 12 milliseconds
    99.81% <= 13 milliseconds
    99.91% <= 14 milliseconds
    99.96% <= 15 milliseconds
    100.00% <= 15 milliseconds
    8665.51 requests per second


    $ redis-benchmark -p 16379 -n 1000 lpush mylist __rand_int__  -c 50
    ====== lpush mylist __rand_int__ -c 50 ======
      1000 requests completed in 0.13 seconds
      50 parallel clients
      3 bytes payload
      keep alive: 1
    
    0.10% <= 2 milliseconds
    0.60% <= 3 milliseconds
    2.00% <= 4 milliseconds
    5.80% <= 5 milliseconds
    54.50% <= 6 milliseconds
    66.00% <= 7 milliseconds
    91.10% <= 8 milliseconds
    96.20% <= 9 milliseconds
    96.90% <= 10 milliseconds
    98.20% <= 11 milliseconds
    99.60% <= 12 milliseconds
    100.00% <= 12 milliseconds
    7462.69 requests per second


    redis-benchmark -p 16379 -n 10000 -r 1000000 hset myhash __rand_int__  -c 50
    ====== hset myhash __rand_int__ -c 50 ======
      10000 requests completed in 1.47 seconds
      50 parallel clients
      3 bytes payload
      keep alive: 1
    
    0.01% <= 1 milliseconds
    0.20% <= 2 milliseconds
    0.81% <= 3 milliseconds
    0.96% <= 4 milliseconds
    2.75% <= 5 milliseconds
    5.35% <= 6 milliseconds
    27.86% <= 7 milliseconds
    94.87% <= 8 milliseconds
    97.90% <= 9 milliseconds
    98.23% <= 10 milliseconds
    99.01% <= 11 milliseconds
    99.12% <= 12 milliseconds
    99.31% <= 13 milliseconds
    99.66% <= 14 milliseconds
    99.90% <= 15 milliseconds
    99.97% <= 16 milliseconds
    100.00% <= 16 milliseconds
    6793.48 requests per second







##after fastUtil:
    $ ./redis-benchmark.exe -h 192.168.1.236 -p 16379 -t set  -n 100000 -r 100000 -c 200
    ====== SET ======
      100000 requests completed in 4.84 seconds
      200 parallel clients
      3 bytes payload
      keep alive: 1
    
    0.00% <= 3 milliseconds
    0.02% <= 4 milliseconds
    0.30% <= 5 milliseconds
    1.00% <= 6 milliseconds
    3.58% <= 7 milliseconds
    22.48% <= 8 milliseconds
    44.44% <= 9 milliseconds
    56.08% <= 10 milliseconds
    80.75% <= 11 milliseconds
    95.12% <= 12 milliseconds
    97.35% <= 13 milliseconds
    98.43% <= 14 milliseconds
    98.97% <= 15 milliseconds
    99.18% <= 16 milliseconds
    99.24% <= 17 milliseconds
    99.31% <= 18 milliseconds
    99.38% <= 19 milliseconds
    99.48% <= 20 milliseconds
    99.56% <= 21 milliseconds
    99.57% <= 22 milliseconds
    99.64% <= 23 milliseconds
    99.69% <= 24 milliseconds
    99.71% <= 25 milliseconds
    99.74% <= 26 milliseconds
    99.75% <= 27 milliseconds
    99.75% <= 28 milliseconds
    99.79% <= 29 milliseconds
    99.80% <= 32 milliseconds
    99.80% <= 33 milliseconds
    99.81% <= 39 milliseconds
    99.82% <= 40 milliseconds
    99.85% <= 41 milliseconds
    99.91% <= 42 milliseconds
    99.97% <= 43 milliseconds
    99.99% <= 44 milliseconds
    100.00% <= 44 milliseconds
    20673.97 requests per second


    
    
    $ ./redis-benchmark.exe -h 192.168.1.236 -p 16379 -n 1000 lpush mylist __rand_int__  -c 200
    ====== lpush mylist __rand_int__ -c 200 ======
      1000 requests completed in 0.06 seconds
      50 parallel clients
      3 bytes payload
      keep alive: 1
    
    0.10% <= 1 milliseconds
    7.70% <= 2 milliseconds
    60.90% <= 3 milliseconds
    89.10% <= 4 milliseconds
    97.70% <= 5 milliseconds
    100.00% <= 5 milliseconds
    16393.44 requests per second
    
    
    
    $ ./redis-benchmark.exe -h 192.168.1.236 -p 16379 -n 10000 -r 1000000 hset myhash __rand_int__  -c 200
    ====== hset myhash __rand_int__ -c 200 ======
      10000 requests completed in 0.64 seconds
      50 parallel clients
      3 bytes payload
      keep alive: 1
    
    0.01% <= 1 milliseconds
    8.78% <= 2 milliseconds
    44.04% <= 3 milliseconds
    82.60% <= 4 milliseconds
    96.46% <= 5 milliseconds
    98.92% <= 6 milliseconds
    99.46% <= 7 milliseconds
    99.95% <= 8 milliseconds
    100.00% <= 8 milliseconds
    15503.88 requests per second
    
    
    
    
    
