import sys
import os
from settings import beaver_broker_ip, beaver_broker_port, tikv_pd_ip, ycsb_port, ansibledir, deploydir, index_forsearch, pb_forsearch
import psutil
import time
import numpy as np
import requests

#MEM_MAX = psutil.virtual_memory().total
MEM_MAX = 0.8*32*1024*1024*1024                 # memory size of tikv node, not current PC


#------------------knob controller------------------

# disable_auto_compactions
def set_disable_auto_compactions(ip, port, val):
    cmd="./tikv-ctl --host "+ip+":"+port+" modify-tikv-config -m kvdb -n default.disable_auto_compactions -v "+str(val)
    res=os.popen(cmd).read()                        # will return "success"
    return(res)

knob_set=\
    {
    "--max_concurrency_tasks_per_search":
        {
            "changebyyml": True,
            "set_func": None,
            "minval": 0,                           # if type==int, indicate min possible value
            "maxval": 0,                         # if type==int, indicate max possible value
            "enumval": [4, 6, 8],        # if type==enum, list all valid values
            "type": "enum",                          # int / enum
            "default": 0                           # default value
        },
    "--max_per_search_ram":
        {
            "changebyyml": True,
            "set_func": None,
            "minval": 0,                          # if type==int, indicate min possible value
            "maxval": 0,                         # if type==int, indicate max possible value
            "enumval": [198],      # if type==enum, list all valid values
            "type": "enum",                          # int / enum
            "default": 0                            # default value
        },
    "--max_per_sub_search_ram":
        {
            "changebyyml": True,
            "set_func": None,
            "minval": 0,                            # if type==int, indicate min possible value
            "maxval": 0,                            # if type==int, indicate max possible value
            "enumval": [99],            # if type==enum, list all valid values
            "type": "enum",                         # int / enum
            "default": 0                            # default value
        },
    "--block_ids_per_batch":
        {
            "changebyyml": True,
            "set_func": None,
            "minval": 0,                            # if type==int, indicate min possible value
            "maxval": 0,                            # if type==int, indicate max possible value
            "enumval": [16, 18, 20],           # if type==enum, list all valid values
            "type": "enum",                         # int / enum
            "default": 0                            # default value
        },
    "--lease_timeout":
        {
            "changebyyml": True,
            "set_func": None,
            "minval": 0,                            # if type==int, indicate min possible value
            "maxval": 0,                            # if type==int, indicate max possible value
            "enumval": [4, 8, 16, 32, 64],          # if type==enum, list all valid values
            "type": "enum",                         # int / enum
            "default": 0                            # default value
        },
    "--enable_query_cache":
        {
            "changebyyml": True,
            "set_func": None,
            "minval": 0,                            # if type==int, indicate min possible value
            "maxval": 0,                            # if type==int, indicate max possible value
            "enumval": ['false', 'true'],                # if type==enum, list all valid values
            "type": "bool",                         # int / enum
            "default": 0                            # default value
        },
    "rocksdb.writecf.bloom-filter-bits-per-key":
        {
            "changebyyml": True,
            "set_func": None,
            "minval": 0,                            # if type==int, indicate min possible value
            "maxval": 0,                            # if type==int, indicate max possible value
            "enumval": [5,10,15,20],                # if type==enum, list all valid values
            "type": "enum",                         # int / enum
            "default": 0                            # default value
        },
    "rocksdb.writecf.optimize-filters-for-hits":
        {
            "changebyyml": True,
            "set_func": None,
            "minval": 0,                            # if type==int, indicate min possible value
            "maxval": 0,                            # if type==int, indicate max possible value
            "enumval": ['false', 'true'],           # if type==enum, list all valid values
            "type": "bool",                         # int / enum
            "default": 0                            # default value
        },
    }


#------------------metric controller------------------

def read_write_throughput(ip, port):
    return(0)           # DEPRECATED FUNCTION: throughput is instant and could be read from go-ycsb. No need to read in this function

def read_write_latency(ip, port):
    return(0)           # DEPRECATED FUNCTION: latency is instant and could be read from go-ycsb. No need to read in this function

def read_get_throughput(ip, port):
    return(0)           # DEPRECATED FUNCTION: throughput is instant and could be read from go-ycsb. No need to read in this function

def read_get_latency(ip, port):
    return(0)           # DEPRECATED FUNCTION: latency is instant and could be read from go-ycsb. No need to read in this function

def read_scan_throughput(ip, port):
    return(0)           # DEPRECATED FUNCTION: throughput is instant and could be read from go-ycsb. No need to read in this function

def read_scan_latency(ip, port):
    return(0)           # DEPRECATED FUNCTION: latency is instant and could be read from go-ycsb. No need to read in this function

def read_store_size(ip, port):
    return(0)

def read_compaction_cpu(ip, port):
    cmd="ps -aux|grep beaver_datanode|grep -v 'grep'|grep -v '/bin/sh'|awk -F' *' '{print $3}'"
    res=os.popen(cmd).read()
    if len(res) == 0:
        return 0
    else:
        return(res)

def read_compaction_mem(ip, port):
    cmd="ps -aux|grep beaver_datanode|grep -v 'grep'|grep -v '/bin/sh'|awk -F' *' '{print $4}'"
    res=os.popen(cmd).read()
    if len(res) == 0:
        return 0
    else:
        return(res)

def read_search_latency(ip, port):
    url = "http://"+ip+":"+port+"/_search?index="+index_forsearch+"&sid=test&rpc_timeout=60"
    data = pb_forsearch
    testnum = 20
    num = 100
    restime = []
    # costime = []
    for i in range(num + testnum):
        start_api = beaverrequest(url, data)
        if i >= testnum:
            restime.append(start_api[1])
            # costime.append(start_api[0]["timecost"])
    sortedRestime = sorted(restime)
    newrestime = sortedRestime[:-1]
    return sum(newrestime) / len(newrestime)

def beaverrequest(url, data):
    r = requests.post(url, data=data)
    return [r.json(), r.elapsed.total_seconds(), r.status_code]

metric_set=\
    {"write_throughput":
         {
         "read_func": read_write_throughput,
         "lessisbetter": 0,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #incremental
         },
    "write_latency":
        {
         "read_func": read_write_latency,
         "lessisbetter": 1,                    # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #instant
        },
    "get_throughput":
        {
         "read_func": read_get_throughput,
         "lessisbetter": 0,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #incremental
        },
    "get_latency":
        {
         "read_func": read_get_latency,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #instant
        },
    "scan_throughput":
        {
         "read_func": read_scan_throughput,
         "lessisbetter": 0,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #incremental
        },
    "scan_latency":
        {
         "read_func": read_scan_latency,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #instant
        },
    "store_size":
        {
         "read_func": read_store_size,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #instant
        },
    "compaction_cpu":
        {
         "read_func": read_compaction_cpu,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #incremental
        },
    "compaction_mem":
        {
         "read_func": read_compaction_mem,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #incremental
        },
    "search_latency":
        {
         "read_func": read_search_latency,
         "lessisbetter": 1,                   # whether less value of this metric is better(1: yes)
         "calc": "ins",                       #incremental
        },
    }


#------------------workload controller------------------

def run_workload(wl_type):
    #./go-ycsb run tikv -P ./workloads/smallpntlookup -p tikv.pd=192.168.1.130:2379
    cmd="./go-ycsb run tikv -P ./workloads/"+wl_type+" -p tikv.pd="+tikv_pd_ip+':'+ycsb_port+" --threads=512"
    print(cmd)
    res=os.popen(cmd).read()
    return(res)

def load_workload(wl_type):
    #./go-ycsb load tikv -P ./workloads/smallpntlookup -p tikv.pd=192.168.1.130:2379
    # cmd="./tikv-ctl --host "+tikv_ip+":"+tikv_port+" modify-tikv-config -m kvdb -n default.disable_auto_compactions -v 1"
    # tmp=os.popen(cmd).read()                        # will return "success"
    cmd="./go-ycsb load tikv -P ./workloads/"+wl_type+" -p tikv.pd="+tikv_pd_ip+':'+ycsb_port+" --threads=512"
    print(cmd)
    res=os.popen(cmd).read()
    # cmd="./tikv-ctl --host "+tikv_ip+":"+tikv_port+" compact -d kv --threads=512"
    # tmp=os.popen(cmd).read()                        # will return "success"
    return(res)


#------------------common functions------------------

def set_tikvyml(knob_sessname, knob_val):
    ymldir=os.path.join(ansibledir,"conf","beaver_test.gflags_new")
    tmpdir=os.path.join(ansibledir,"conf","beaver_test.gflags")
    with open(tmpdir, 'r') as read_file, open(ymldir, 'w') as write_file:
        dic={}
        for line in read_file:
            value = line.strip().split("=")
            if len(value) > 1:
                dic[value[0]] = value[1]
        if(knob_set[knob_sessname]['type']=='enum'):
            idx=knob_val
            knob_val=knob_set[knob_sessname]['enumval'][idx]
        if(knob_set[knob_sessname]['type']=='bool'):
            if(knob_val==0):
                knob_val='false'
            else:
                knob_val='true'
        if(knob_sessname=='--max_shard_size'):
            knob_val=str(knob_val)+"g"
        if(knob_sessname=='--max_per_search_ram' or knob_sessname=='--max_per_sub_search_ram'):
            knob_val=str(knob_val)+"m"
        if(knob_sessname in dic):
            dic[knob_sessname] = knob_val
        else:
            return('failed')
        print("set_beaver_datanode_gflags:: ",knob_sessname, knob_val)
        for kkk in dic:
            write_file.write(kkk+"="+str(dic[kkk])+'\n')
    # os.popen("rm "+tmpdir+" && "+"mv "+ymldir+" "+tmpdir)
    os.remove(tmpdir)
    os.rename(ymldir, tmpdir)
    time.sleep(0.5)
    return('success')
    
    
    # if(knob_name=='block-size'):
    #     knob_val=str(knob_val)+"KB"
    # if(knob_name=='write-buffer-size' or knob_name=='max-bytes-for-level-base' or knob_name=='target-file-size-base'):
    #     knob_val=str(knob_val)+"MB"
    # if(knob_name in tmpcontent[knob_sess[0]][knob_sess[1]]):        # TODO: only support 2 level of knob_sess currently
    #     tmpcontent[knob_sess[0]][knob_sess[1]][knob_name]=knob_val
    # else:
    #     return('failed')
    # print("set_tikvyml:: ",knob_sessname, knob_sess, knob_name, knob_val)
    # ymlf=open(ymldir, 'w')
    # yaml.dump(tmpcontent, ymlf, Dumper=yaml.RoundTripDumper)
    # os.popen("rm "+tmpdir+" && "+"mv "+ymldir+" "+tmpdir)
    # time.sleep(0.5)
    # return('success')

def set_knob(knob_name, knob_val):
    changebyyml=knob_set[knob_name]["changebyyml"]
    if(changebyyml):
        res=set_tikvyml(knob_name, knob_val)
    else:
        func=knob_set[knob_name]["set_func"]
        res=func(beaver_broker_ip, beaver_broker_port, knob_val)
    return res

def read_knob(knob_name, knob_cache):
    res=knob_cache[knob_name]
    return res

def read_metric(metric_name, rres=None):
    if(rres!=None):
        rl=rres.split('\n')
        rl.reverse()
        if(metric_name=="write_latency"):
            i=0
            while((not rl[i].startswith('UPDATE ')) and (not rl[i].startswith('INSERT '))):
                i+=1
            dat=rl[i][rl[i].find("Avg(us):") + 9:].split(",")[0]
            dat=int(dat)
            return(dat)
        elif(metric_name=="get_latency"):
            i=0
            while(not rl[i].startswith('READ ')):
                i+=1
            dat=rl[i][rl[i].find("Avg(us):") + 9:].split(",")[0]
            dat=int(dat)
            return(dat)
        elif(metric_name=="scan_latency"):
            i=0
            while(not rl[i].startswith('SCAN ')):
                i+=1
            dat=rl[i][rl[i].find("Avg(us):") + 9:].split(",")[0]
            dat=int(dat)
            return(dat)
        elif(metric_name=="write_throughput"):
            i=0
            while((not rl[i].startswith('UPDATE ')) and (not rl[i].startswith('INSERT '))):
                i+=1
            dat=rl[i][rl[i].find("OPS:") + 5:].split(",")[0]
            dat=float(dat)
            return(dat)
        elif(metric_name=="get_throughput"):
            i=0
            while(not rl[i].startswith('READ ')):
                i+=1
            dat=rl[i][rl[i].find("OPS:") + 5:].split(",")[0]
            dat=float(dat)
            return(dat)
        elif(metric_name=="scan_throughput"):
            i=0
            while(not rl[i].startswith('SCAN ')):
                i+=1
            dat=rl[i][rl[i].find("OPS:") + 5:].split(",")[0]
            dat=float(dat)
            return(dat)
    func=metric_set[metric_name]["read_func"]
    res=func(beaver_broker_ip, beaver_broker_port)
    return res

def init_knobs():
    # if there are knobs whose range is related to PC memory size, initialize them here
    pass

def calc_metric(metric_after, metric_before, metric_list):
    num_metrics = len(metric_list)
    new_metric = np.zeros([1, num_metrics])
    for i, x in enumerate(metric_list):
        if(metric_set[x]["calc"]=="inc"):
            new_metric[0][i]=metric_after[0][i]-metric_before[0][i]
        elif(metric_set[x]["calc"]=="ins"):
            new_metric[0][i]=metric_after[0][i]
    return(new_metric)

def restart_db():
    #cmd="cd /home/tidb/tidb-ansible/ && ansible-playbook unsafe_cleanup_data.yml"
    dircmd="cd "+ ansibledir + " && "
    clrcmd="ansible-playbook unsafe_cleanup_data.yml"
    depcmd="ansible-playbook deploy.yml"
    runcmd="ansible-playbook start.yml"
    ntpcmd="ansible-playbook -i hosts.ini deploy_ntp.yml -u tidb -b"   #need sleep 10s after ntpcmd
    print("-------------------------------------------------------")
    clrres = os.popen(dircmd+clrcmd).read()
    if("Congrats! All goes well" in clrres):
        print("unsafe_cleanup_data finished, res == "+clrres.split('\n')[-2])
    else:
        print(clrres)
        print("unsafe_cleanup_data failed")
        exit()
    print("-------------------------------------------------------")
    ntpres = os.popen(dircmd + ntpcmd).read()
    time.sleep(10)
    if ("Congrats! All goes well" in ntpres):
        print("set ntp finished, res == " + ntpres.split('\n')[-2])
    else:
        print(ntpres)
        print("set ntp failed")
        exit()
    print("-------------------------------------------------------")
    depres = os.popen(dircmd + depcmd).read()
    if ("Congrats! All goes well" in depres):
        print("deploy finished, res == "+depres.split('\n')[-2])
    else:
        print(depres)
        print("deploy failed")
        exit()
    print("-------------------------------------------------------")
    runres = os.popen(dircmd + runcmd).read()
    if ("Congrats! All goes well" in runres):
        print("start finished, res == "+runres.split('\n')[-2])
    else:
        print(runres)
        print("start failed")
        exit()
    print("-------------------------------------------------------")

def restart_beaver():
    dircmd="cd "+ ansibledir + " && "
    stopcmd="ps -ef|grep beaver_datanode|grep -v 'grep'|awk -F' *' '{print $2}'|xargs kill"
    querycmd="ps -ef|grep beaver_datanode|grep -v 'grep'|awk -F' *' '{print $2}'"
    beaver_conf=os.path.join(ansibledir,"conf","beaver_datanode.gflags")
    test_conf=os.path.join(ansibledir,"conf","beaver_test.gflags")
    startcmd="/opt/rizhiyi/parcels/beaver_datanode-3.7.0.0/bin/beaver_datanode --flagfile="+beaver_conf+" "+"--config_path=/run/rizhiyi_manager_agent/process/2002-beaver_datanode/config/beaver_datanode.pb --log_dir=/data/rizhiyi/logs/beaver_datanode > /dev/null 2>&1"
    print("-----------------------------stop beaver datanode--------------------------")
    stopres = os.popen(stopcmd).read()
    if len(os.popen(querycmd).read()) != 0:
        for i in range(5):
            time.sleep(2)
            psres = os.popen(querycmd).read()
            if len(psres) == 0 :
                print("Beaver has been closed successfully!")
                break
            else:
                print("Waiting beaver to close, pid is %s" % psres)
            if i == 4:
                print("Beaver close failed!")
                exit()
    else:
        print("Beaver closed successfully!")
    print("-----------------------------replace config file--------------------------")
    if os.path.exists(beaver_conf):
        os.remove(beaver_conf)
    replaceres = os.popen("cp "+test_conf+" "+beaver_conf).read()
    if len(replaceres) == 0:
        print("replace config file finished!")
    else:
        print(replaceres)
        print("replace config file failed!")
        exit()
    print("-----------------------------start beaver datanode--------------------------")
    startres = os.popen(startcmd)
    beaver_url = "http://"+beaver_broker_ip+":"+beaver_broker_port+"/_search?index="+index_forsearch+"&sid=test&rpc_timeout=60"
    for i in range(20):
        time.sleep(10)
        curlres = requests.post(beaver_url, data=pb_forsearch).json()
        if "result" in curlres and curlres['result'] == False:
            print("Waiting beaver datanode to be available...")
        else:
            print("Beaver datanode is available!")
            break
        if i == 19:
            print(curlres)
            print("Beaver start failed!")
            exit()
    print("---------------------------------------------------------------------------")


