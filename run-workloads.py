import os
import subprocess
import time

MONGOD_CMD = "mongod"
DBPATH = "/root/data/db"
MONGO_HOME = "/root/mongo"
MONGO_SRC = f"{MONGO_HOME}/src/mongo/db"
MONGO_OBJ = f"{MONGO_HOME}/build/opt/mongo/db"
OUTPUT_DIR = "/root/scripts/OUTPUTS"
WORKLOADS = ["workloada", "workloadb", "workloadc", "workloadd", "workloade", "workloadf"]
YCSB_HOME = "/root/ycsb-0.17.0"

ycsb = open("/root/scripts/OUTPUTS/ycsb-dump.txt", "w+")

def start_mongod(): 
    print("Cleared mongodb database")
    subprocess.run(["rm", "-r", f"{DBPATH}"], check=False) # Continue running even if rm fails
    os.chdir("/root/scripts")
    os.makedirs(DBPATH, exist_ok=True)
    print("Started mongod server")
    mongod = subprocess.Popen([MONGOD_CMD, f"--dbpath={DBPATH}"], stdout=subprocess.PIPE, shell=False)
    time.sleep(5)
    return mongod

def kill_mongod(pid):
    subprocess.run(["kill", "-INT", f"{pid}"], check=True)
    print(f"Killed mongod")

def clear_gcda_files(check=True):
    subprocess.run([f"find {MONGO_OBJ} -name '*.gcda' -delete"], check=check, shell=True)
    print(f"Deleted all gcda files in {MONGO_OBJ}")

def run_ycsb_workload(workload, recordcount=50000, threads=8):
    pid = start_mongod().pid
    for wrk_type in ["load", "run"]:
        cmd = f"ycsb {wrk_type} mongodb -s -P workloads/{workload} -p recordcount={recordcount} -threads {threads}".split()
        process = subprocess.run(cmd, check=True, cwd=YCSB_HOME, capture_output=True)
        print("Ran ycsb: ", " ".join(cmd))
        print(process.stdout.decode('utf-8'), file=ycsb)
        print("\n\n", file=ycsb)
    kill_mongod(pid)
    time.sleep(20) # wait after ending mongod for gcov files to be generated

def capture_gcov_output(output_dir):
    """
        Runs the gcov command and saves the output to `output_file`.
        Flags:
            -b: show branch probabilities
            -f: provide function summary
            -m: demangle function names
            -n: no-output (Doesn't output line by line coverage to stdout or a .gcov file)
            -d: display progress
            -r: relative-only (ignore linked libraries, etc)
    """
    os.chdir("/root/scripts")
    os.makedirs(output_dir, exist_ok=True) 
    os.chdir(MONGO_HOME)

    count = 0
    for curdir, _, files in os.walk(MONGO_OBJ):
        for file in files:
            if file.endswith(".gcda"):
                count+=1
                gcda_filename = os.path.join(curdir, file)
                gcda_filename = os.path.relpath(gcda_filename, MONGO_OBJ)
                cpp_filename =  gcda_filename.replace(".gcda", ".cpp")
                obj_filename = gcda_filename.replace(".gcda", ".o")

                cmd = f"gcov {cpp_filename} -o {MONGO_OBJ}/{obj_filename} -b -f -m -n -d -r".split()
                process = subprocess.run(cmd, capture_output=True, check=True)
                output = process.stdout.decode('utf-8')
                file = os.path.basename(cpp_filename)
                with open(f"{output_dir}/{file}.gcov", "w+") as f:
                    print(output, file=f)
    print(f"Captured gcov output for {count} files...")

def main():
    progress = 0
    clear_gcda_files(check=False)
    for workload in WORKLOADS:
        progress+=1
        print(f"Progress ({progress}/{6}): [{'#'*progress*2}{'_'*(6-progress)*2}]")
        run_ycsb_workload(workload)
        capture_gcov_output(f"{OUTPUT_DIR}/{workload}")
        clear_gcda_files(check=False)


if __name__ == "__main__":
    main()
    ycsb.close()