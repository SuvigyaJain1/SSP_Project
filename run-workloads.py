import os
import subprocess
import time

MONGOD_CMD = "mongod"
DBPATH = "/root/data/db"
MONGO_HOME = "/root/mongo"
MONGO_SRC = f"{MONGO_HOME}/src/mongo/db"
MONGO_OBJ = f"{MONGO_HOME}/build/opt/mongo/db"
OUTPUT_DIR = "OUTPUTS"
WORKLOADS = ["workloada", "workloadb", "workloadc", "workloadd", "workloade", "workloadf"]

def start_mongod(): 
    print("Cleared mongodb database")
    subprocess.run(["rm", "-r", f"{DBPATH}"])
    print("Started mongod server")
    mongod = subprocess.Popen([MONGOD_CMD, f"--dbpath={DBPATH}"], stdout=subprocess.PIPE, shell=False, check=True)
    return mongod

def kill_mongod(pid):
    subprocess.run(["kill", "-9", f"{pid}"], check=True)
    print(f"Killed down mongod")

def clear_gcda_files():
    subprocess.run(["rm", f"{MONGO_OBJ}/*.gcda"], check=True)
    print("Deleted all gcda files")

def run_ycsb_workload(workload, recordcount=50000, threads=8, async_run=False):
    pid = start_mongod()
    time.sleep(5)
    target = "mongodb-async" if async_run else "mongodb" 
    for wrk_type in ["load", "run"]:
        cmd = f"ycsb {wrk_type} {target} -s -P workloads/{workload} -p recordcount={recordcount} -threads {threads}".split()
        subprocess.run(cmd, check=True)
    kill_mongod(pid)

def capture_gcov_output(output_dir):
    """
        Runs the gcov command and saves the output to `output_file`.
        Flags:
            -b
            -f: provide function summary
            -m: demangle function names
            -n: no-output (Doesn't output line by line coverage to stdout or a .gcov file)
            -d: 
            -r
    """
    cpp_files = list(filter(lambda x: x.endswith(".cpp"), os.listdir(MONGO_SRC)))
    os.chdir(MONGO_HOME)
    os.makedir(output_dir, exist_ok=True) 

    for file in cpp_files:
        cmd = f"gcov {filepath} -o {MONGO_OBJ} -b -f -m -n -d -r".split()
        process = subprocess.run(cmd, capture_output=True, check=True)
        output = process.stdout.decode('utf-8')
        with open(f"{output_dir}/{file}.gcov", "w") as f:
            print(output, file=f)
     
def main():

    for workload in WORKLOADS:
        for async_run in [True, False]:
            run_ycsb_workload(workload, async_run=async_run)
            capture_gcov_output(f"{OUTPUT_DIR}/{workload}/{"async" if async_run else "sync"}")
            clear_gcda_files()


if __name__ == "__main__":
    main()