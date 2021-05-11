import os
from controller import read_metric, read_knob, set_knob, restart_beaver_datanode, read_search_latency
import requests

def file_handle(filename):
    new_file=filename+'_new'
    bak_file=filename+'_bak'
    with open(filename, 'r') as read_file, open(new_file, 'w') as write_file:
        i={}
        for line in read_file:
            value = line.strip().split("=")
            if len(value) > 1:
                i[value[0]] = value[1]
                if("--cluster_namea" in i):
                    i['--cluster_name'] = "qibohan"
                    # print(i["--cluster_name"])
        # print(i)
        # for kkk in i:
        #     #print(kkk+"="+i[kkk])
        #     write_file.write(kkk+"="+i[kkk]+'\n')


if __name__ == '__main__':
    # filename = '/tmp/beaver_datanode.gflags'
    # file_handle(filename)
    # set_knob("--max_per_search_ram", 0)
    restart_beaver_datanode()
    # aaa = read_search_latency("172.21.16.16", "50061")
    # print(type(aaa))
    # print(aaa)
    # url = "http://172.21.16.16:50061/_search?pretty=true&index=ops-http_baimi-20210507&sid=test&rpc_timeout=60"
    # pbdata = 'search_info {query {type: kQueryMatchAll}fetch_source {fetch: true}size {value: 0} aggregations { aggs { type: kAggAvg name: "av(apache.resp_len)" body { field: "apache.resp_len__l__" } } } query_time_range {time_range {min: 0 max: 1620374828405}}}'
    # curlres = requests.post(url, data=pbdata)
    # print(curlres.elapsed.total_seconds())
    # print(curlres.json()["timecost"])
    # if "result" in curlres and curlres['result'] == False:
    #     print(curlres)


