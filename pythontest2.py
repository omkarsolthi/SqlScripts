from sys import stdout
from time import sleep
for i in range(1,20):
    print( i)
    stdout.flush()
    sleep(1)
stdout.write("\n")

