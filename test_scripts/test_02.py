#!/usr/bin/python3

import argparse
import os
import subprocess
import sys
import time
import datetime

chunk_size=8192
filepath = os.path.dirname(os.path.abspath(__file__))



def execute_ceph():
    os.chdir(ceph_bin_abs_path + '/../')
    ceph_log = open("ceph.log", "w")
    subprocess.call("sudo rm -r dev out", shell=True)
    subprocess.call("sudo CEPH_NUM_FS=0 MON=1 OSD=1 ../src/vstart.sh --new --without-dashboard --bluestore-devs /dev/nvme0n1", shell=True, stderr=ceph_log, stdout=ceph_log)
    subprocess.call("sudo cp ceph.conf" + filepath, shell=True)

def configure_ceph():
    os.chdir(ceph_bin_abs_path + '/../')                                                                          
    subprocess.call("sudo bin/ceph osd pool create base_pool 128", shell=True)                                    
    subprocess.call("sudo bin/ceph osd pool create chunk_pool", shell=True)                                       
    subprocess.call("sudo bin/ceph osd pool set base_pool dedup_tier chunk_pool", shell=True)                     
    subprocess.call("sudo bin/ceph osd pool set base_pool dedup_chunk_algorithm fastcdc", shell=True)             
    subprocess.call("sudo bin/ceph osd pool set base_pool dedup_cdc_chunk_size " + str(chunk_size), shell=True)   
    subprocess.call("sudo bin/ceph osd pool set base_pool fingerprint_algorithm sha1", shell=True)                
    subprocess.call("sudo bin/ceph osd pool set base_pool pg_autoscale_mode off", shell=True)                     
    subprocess.call("sudo bin/ceph osd pool set base_pool target_max_objects 1", shell=True)
    subprocess.call("sudo bin/ceph osd pool create rbd", shell=True)                 
    subprocess.call("sudo bin/ceph osd pool set rbd size 1 --yes-i-really-mean-it", shell=True) 
    subprocess.call("sudo bin/rbd -p rbd create --size 1024 fio_test", shell=True)    
    subprocess.call("sudo bin/rbd map fio_test", shell=True);


def excute_shallowcrawler(time_file):
    os.chdir(ceph_bin_abs_path + '/../')                                                                          
    command = "sudo bin/ceph-dedup-tool --op sample-dedup --base-pool base_pool --chunk-pool chunk_pool --max-thread 1 --shallow-crawling --sampling-ratio 10 --osd-count 1"
    print("execute shallow crawler " + str(datetime.datetime.now()))
    time_file.write(str(datetime.datetime.now())+"\n")
    subprocess.call(command, shell=True)

def excute_deepcrawler(time_file):
    os.chdir(ceph_bin_abs_path + '/../')                                                                          
    command = "sudo bin/ceph-dedup-tool --op sample-dedup --base-pool base_pool --chunk-pool chunk_pool --max-thread 1 --sampling-ratio 100 --osd-count 1"
    print("execute deep crawler " + str(datetime.datetime.now()))
    time_file.write(str(datetime.datetime.now())+"\n")
    subprocess.call(command, shell=True)

def process():
    global ceph_bin_abs_path
    ceph_bin_abs_path =  os.path.abspath(args.ceph)


    print("execute ceph\n")      
    execute_ceph()               
    print("configure ceph\n")    
    configure_ceph()             

    print("excute fio\n")
    fio_log = open("fio.log","w")
    fio_popen = subprocess.Popen("sudo fio fio.cfg", shell=True, stdout=fio_log, stderr=fio_log)
    print("excute sar\n")
    sar_log = open("sar_out.log","w")
    sar_popen = subprocess.Popen("sudo sar -d 1 1000 --dev=dev252-0", shell=True, stdout=sar_log, stderr=sar_log)

    time.sleep(60)

    s_time_file = open("shallow_time.log", "w")
    d_time_file = open("deep_time.log", "w")
    
    i=0
    while True:
        print("execute shallow")
        excute_shallowcrawler(s_time_file)
        i+=1
        if(i==5):
            break
        print("wait 60s\n")
        time.sleep(10)

    time.sleep(10)
    i=0
    while True:
        print("execute shallow")
        excute_deepcrawler(d_time_file)
        i+=1
        if(i==5):
            break
        print("wait 60s\n")
        time.sleep(10)

    subprocess.call("sudo pkill -9 fio", shell=True)
    subprocess.call("sudo pkill -9 sar", shell=True)

    fio_popen.terminate()
    fio_popen.wait()

    sar_popen.terminate()
    sar_popen.wait() 

    subprocess.call("sudo bin/rbd unmap fio_test", shell=True)    


def end_test():
    os.chdir(ceph_bin_abs_path + '/../')                                                                          
    print("stop ceph\n")
    subprocess.call("sudo ../src/stop.sh", shell=True)
    
def parse_arguments():                                             
    parser = argparse.ArgumentParser()                               
    print(parser)                                                    
    parser.add_argument('--ceph', type=str, help='ceph bin path')    
    global args                                                      
    args = parser.parse_args()                                       
    print(args)                                                      


if __name__ == "__main__":
    parse_arguments()
    process()
    end_test()

