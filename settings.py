#info about beaver
beaver_broker_ip="172.21.16.16"
beaver_broker_port="50061"
index_forsearch="ops-http_baimi-20210507"
pb_forsearch='search_info {query {type: kQueryMatchAll}fetch_source {fetch: true}size {value: 0} aggregations { aggs { type: kAggAvg name: "av(apache.resp_len)" body { field: "apache.resp_len__l__" } } } query_time_range {time_range {min: 0 max: 1620374828405}}}'

# workloads and their related performance metrics
wl_metrics={
    "writeheavy":["write_throughput","write_latency","store_size","compaction_cpu"],        #UPDATE
    "pntlookup40": ["get_throughput","get_latency","store_size","compaction_cpu"],          #READ
    "pntlookup80": ["get_throughput","get_latency","store_size","compaction_cpu"],          #READ
    "longscan":  ["scan_throughput","scan_latency","store_size","compaction_cpu"],          #SCAN
    "shortscan": ["scan_throughput","scan_latency","store_size","compaction_cpu"],          #SCAN
    "smallpntlookup": ["get_throughput","get_latency","store_size","compaction_cpu"],       #READ
    "avgsearch": ["search_latency","compaction_mem","compaction_cpu"],
}
# workload to be load
loadtype = "avgsearch"
# workload to be run
wltype = "avgsearch"

# only 1 target metric to be optimized
target_metric_name="search_latency"

# several knobs to be tuned
target_knob_set=['--enable_query_cache',                 # 启用query cache
                 '--max_concurrency_tasks_per_search',   # 每个Search允许同时执行的数目
                 '--max_per_search_ram',                 # 单个Search最大占用的内存
                 '--max_per_sub_search_ram',             # 单个SubSearch最大占用的内存
                 '--block_ids_per_batch']                # 每个SubSearch的Block数目

autotestdir="/tmp/auto_beaver_datanode"
beaver_datanode_file="/opt/rizhiyi/parcels/beaver_datanode-3.7.0.0/bin/beaver_datanode"
gflagsfile="/run/rizhiyi_manager_agent/process/2002-beaver_datanode/config/beaver_datanode.gflags"
config_path="/run/rizhiyi_manager_agent/process/2002-beaver_datanode/config/beaver_datanode.pb"
log_dir="/data/rizhiyi/logs/beaver_datanode"