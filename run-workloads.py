import os
import subprocess

MONGOD_CMD = "mongod"
DBPATH = "/root/data/db"

def start_mongod(): 
    mongod = subprocess.run([MONGOD_CMD, f"--dbpath={DBPATH}"])
    return mongod

def main():
    mongod = start_mongod()
    print("Checking if this runs")
    print(mongod)

if __name__ == "__main__":
    main()