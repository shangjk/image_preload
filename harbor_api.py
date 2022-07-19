import sys
import requests
import time
import configparser
import threading
import base64
import logging
import socket
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler

requests.packages.urllib3.disable_warnings()

logger = logging.getLogger("preload-server")
# 定义logging日志格式配置
log_fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(lineno)d  %(message)s')
logging.basicConfig(filename="/tmp/harbor_api.log", filemode="a",
                    format='%(asctime)s [%(levelname)s] %(lineno)d  %(message)s', level="INFO")

# 定义一个控制台输出的日志器
screen_handler = logging.StreamHandler()
# screen_handler.setLevel("DEBUG") # 该配置不生效
screen_handler.setFormatter(fmt=log_fmt)
logger.addHandler(screen_handler)

# 定义一个apscheduler定时器
jk_scheduler = BackgroundScheduler()


class Harbor(object):
    def __init__(self, url, username, password, project, version):
        """
        初始化一些参数
        :param username: login user
        :param password: login password
        :param url: harbor server api url
        :param project: project name
        :param version: harbor api version (v1.0, v2.0)
        """
        # self.project_id = None
        self.session = requests.Session()
        self.auth = HTTPBasicAuth(username, password)
        self.project_name = project
        self.session.auth = self.auth
        self.session.keep_alive = False
        self.version = version
        retry = Retry(connect=3, backoff_factor=1)
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount('http://', adapter)
        self.Head = {
            "accept": "application/json"
        }
        if self.version == "v2.0":
            self.url = "http://%s" % url + "/api/v2.0/"
        elif self.version == "v1.0":
            self.url = "http://%s" % url + "/api/"

    def get_project_id(self):
        project_dict = {}
        try:
            res = self.session.get(url=self.url + "projects", headers=self.Head)
        except Exception as e:
            logger.error("Exception when calling get %s project_id: %s " % (self.project_name, e))
            return False, None
        else:
            if res.status_code == 200:
                for i in res.json():
                    project_dict[i['name']] = i['project_id']
                if self.project_name in project_dict.keys():
                    return True, project_dict[self.project_name]
                else:
                    logger.error("When get_project_id: no found project_name {} in harbor!".format(self.project_name))
                    return False, None
            else:
                logger.error("When get_project_id: requests failed for search {} id, reponse code: {}".format(self.project_name, res.status_code))
                return False, None

    def get_project_events(self, begin_time, end_time):
        res_events = []
        try:
            if self.version == "v2.0":
                begin = time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime(begin_time)).split("-")
                end = time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime(end_time)).split("-")
                res_url = self.url + "projects/{}/logs?q=operation%3Dcreate%2Cop_time%3D%5B{}-{}-{}%20{}%3A{}%3A{}~{}-{}-{}%20{}%3A{}%3A{}%5D".format(
                    self.project_name, begin[0], begin[1], begin[2], begin[3], begin[4], begin[5], end[0], end[1],
                    end[2], end[3], end[4], end[5])
                res_events = self.session.get(url=res_url, headers=self.Head, timeout=10)
            elif self.version == "v1.0":
                global project_id
                if project_id is None:
                    status, project_id = self.get_project_id()
                else:
                    status = True

                if status:
                    res_url = self.url + "projects/{}/logs?operation=push&begin_timestamp={}&end_timestamp={}".format(project_id, int(begin_time), int(end_time))
                    res_events = self.session.get(url=res_url, headers=self.Head, timeout=10)
                else:
                    logger.error("Failed when calling get_project_id, no return project_id !")
                    return []
        except Exception as e:
            logger.error("Exception when calling get project %s evetns: %s" % (self.project_name, e))
            return []
        else:
            if res_events.status_code == 200:
                return res_events.json()
            else:
                logger.error("Exception when calling get project %s events: %s" % (self.project_name, res_events.json()))
                return []


class Cluster(object):
    def __init__(self, namespace, labels: list):
        self.token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        # self.token_file = "token"
        # self.ca_file = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        self.endpoints_url = "/api/v1/namespaces/{}/endpoints?labelSelector={}%3D{}".format(namespace, labels[0], labels[1])
        self.api_host = "https://192.168.0.1"
        with open(self.token_file, 'r') as a:
            self.token = a.read()

    # 定义方法获取目标容器的IP地址池
    def get_endpoints(self):
        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "Authorization": "Bearer {}".format(self.token)
        }
        target_ip_pool = []
        try:
            endpoints = requests.get(url=self.api_host + self.endpoints_url, headers=headers, verify=False, timeout=5)
        except Exception as e:
            logger.error(e)
            return False, e
        else:
            if endpoints.status_code == 200:
                try:
                    ip_subset = endpoints.json()['items'][0]['subsets'][0]['addresses']
                    for i in ip_subset:
                        target_ip_pool.append(i['ip'])
                    logger.debug("Target endpoints pools : %s" % target_ip_pool)
                except KeyError as e:
                    logger.error("Exception when get_endpoints for preload_client, maybe no pod survive: {}".format(e))
                    return False, "No available pod for use !"
                else:
                    return True, target_ip_pool
            else:
                logger.error(endpoints.text)
                return False, "status_code: {}  messages: {}".format(endpoints.status_code, endpoints.text)


class ElasticsearchClient(object):

    def __init__(self, elasticsearch_index, elasticsearch_hosts, elasticsearch_username="", elasticsearch_password=""):
        self.index_name = elasticsearch_index
        mapping = {
            "mappings": {
                "properties": {
                    "create_time": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy/MM/dd HH:mm:ss",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "image": {
                        "type": "keyword",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "operation": {
                        "type": "keyword",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            },
            "settings": {
                "index": {
                    "number_of_shards": "2",
                    "number_of_replicas": "2"
                }
            }
        }
        try:
            if elasticsearch_username != "" and elasticsearch_password != "":
                self.es_client = Elasticsearch(hosts=elasticsearch_hosts.split(","),
                                               http_auth=(elasticsearch_username, elasticsearch_password), timeout=30)
            else:
                self.es_client = Elasticsearch(hosts=elasticsearch_hosts.split(","), timeout=30)
            check_index = self.es_client.indices.exists(index=self.index_name)
            if check_index is not True:
                self.es_client.indices.create(index=self.index_name, body=mapping)
        except Exception as e:
            logger.error(e)
            sys.exit(10)

    def elastic_write(self, create_time, image_data: str):
        data = {
            "create_time": create_time,
            "image": image_data,
            "operation": "pushed"
        }

        try:
            status, res_num = self.elastic_search(create_time, image_data)
            if not status and res_num == 0:
                self.es_client.index(index=self.index_name, body=data)
        except Exception as e:
            logger.error(e)
            return False
        else:
            return True

    def elastic_search(self, create_time, data: str):
        query = {
            "query": {
                "bool": {
                    "filter": {
                        "bool": {
                            "must": [
                                {"match": {"image": data}},
                                {"match": {"create_time": create_time}}
                            ]
                        }
                    }
                }
            }
        }
        try:
            result = self.es_client.search(index=self.index_name, body=query)
        except Exception as e:
            logger.error(e)
            return False
        else:
            if result["hits"]["total"]["value"] > 0:
                return True, result["hits"]["total"]["value"]
            else:
                return False, result["hits"]["total"]["value"]

    def elastic_search_images(self, begin_time: str, end_time: str):
        query_2 = {
            "query": {
                "bool": {
                    "filter": {
                        "range": {
                            "create_time": {
                                "gte": "{}".format(begin_time),
                                "lt": "{}".format(end_time)
                            }
                        }
                    }
                }
            }
        }
        print(query_2)
        try:
            result = self.es_client.search(index=self.index_name, body=query_2)
        except Exception as e:
            logger.error(e)
            return False, e
        else:
            return True, result


def get_cfg(cfg_file):
    config = configparser.ConfigParser()
    config.read(cfg_file)
    if not config.get("harbor_info", "harbor_addr"):
        return {"res": False, "msg": "{} section harbor_info option harbor_addr is None !".format(cfg_file)}
    elif not config.get("harbor_info", "harbor_username"):
        return {"res": False, "msg": "{} section harbor_info option harbor_username is None !".format(cfg_file)}
    elif not config.get("harbor_info", "harbor_password"):
        return {"res": False, "msg": "{} section harbor_info option harbor_password is None !".format(cfg_file)}
    elif not config.get("harbor_info", "harbor_projects"):
        return {"res": False, "msg": "{} section harbor_info option harbor_projects is None !".format(cfg_file)}
    elif not config.get("harbor_info", "harbor_api_version"):
        return {"res": False, "msg": "{} section harbor_info option harbor_api_version is None !".format(cfg_file)}
    elif not config.get("harbor_daemon", "listen_interval"):
        return {"res": False, "msg": "{} section harbor_daemon option listen_interval is None !".format(cfg_file)}
    elif not config.get("harbor_daemon", "docker_daemon_label"):
        return {"res": False, "msg": "{} section harbor_daemon option docker_daemon_label is None !".format(cfg_file)}
    elif not config.get("harbor_daemon", "docker_daemon_namespace"):
        return {"res": False, "msg": "{} section harbor_daemon option docker_daemon_namespace is None !".format(cfg_file)}
    elif not config.get("harbor_daemon", "elasticsearch_hosts"):
        return {"res": False, "msg": "{} section harbor_daemon option elasticsearch_hosts is None !".format(cfg_file)}
    elif not config.get("harbor_daemon", "crontab"):
        return {"res": False, "msg": "{} section harbor_daemon option crontab is None !".format(cfg_file)}
    elif not config.get("harbor_daemon", "elasticsearch_index"):
        return {"res": False, "msg": "{} section harbor_daemon option elasticsearch_index is None !".format(cfg_file)}
    else:
        return {"res": True, "msg": config}


def post_images_info(ip_addr, images: dict):
    headers = {
        "Content-Type": "application/json"
    }
    try:
        result = requests.post(url="http://{}:8000/pull".format(ip_addr), headers=headers, json=images, timeout=120)
    except Exception as e:
        return {"status": False, "message": e}
    else:
        if result.status_code == 200:
            return {"status": True, "message": result.text}
        else:
            return {"status": False, "message": result.text}


def check_connection(ip, port):
    so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    so.settimeout(3)
    try:
        so.connect((ip, int(port)))
        so.shutdown(socket.SHUT_RDWR)
        return True
    except Exception as e:
        logger.debug("Exception when check connection %s %s : %s" % (ip, port, e))
        return False


def utc_gmt(timestamp_iso8601):
    """
    将iso861timestamp转换为北京时间
    :return:
    """
    if "." not in timestamp_iso8601:  # 当毫秒为0
        utc = datetime.strptime(timestamp_iso8601, '%Y-%m-%dT%H:%M:%SZ')
    else:
        utc = datetime.strptime(timestamp_iso8601, '%Y-%m-%dT%H:%M:%S.%fZ')
    gmt_time = (utc + timedelta(hours=8)).replace(microsecond=0)
    return gmt_time


def main():
    logger.info("{} Start Begin {}".format("="*30, "="*30))

    # 定义一个获取线程返回结果变量
    def result_result(future):
        results = future.result(timeout=180)
        if results['status']:
            logger.info("[{}] {}".format(threading.current_thread().name, results['message']))
        else:
            logger.error("[{}] {}".format(threading.current_thread().name, results['message']))

    def result_result2(future):
        results = future.result(timeout=1800)
        if results['status']:
            logger.info("[{}] {}".format(threading.current_thread().name, results['message']))
        else:
            logger.error("[{}] {}".format(threading.current_thread().name, results['message']))

    # 加载配置文件，并判断必要参数是否为空
    cfg = get_cfg('/etc/preload/harbor_api.cfg')
    if cfg['res']:
        configs = cfg['msg']
        # 加载变量
        harbor_addr = configs.get("harbor_info", "harbor_addr")
        harbor_scheme = configs.get("harbor_info", "harbor_scheme")
        harbor_username = configs.get("harbor_info", "harbor_username")
        harbor_password = base64.b64decode(configs.get("harbor_info", "harbor_password"))
        harbor_projects = configs.get("harbor_info", "harbor_projects")
        harbor_api_version = configs.get("harbor_info", "harbor_api_version")
        listen_interval = int(configs.get("harbor_daemon", "listen_interval"))
        docker_daemon_namespace = configs.get("harbor_daemon", "docker_daemon_namespace")
        docker_daemon_label = configs.get("harbor_daemon", "docker_daemon_label")
        thread_pools = configs.get("harbor_daemon", "thread_pools")
        action_timeout = configs.get("harbor_daemon", "action_timeout")
        elasticsearch_index = configs.get("harbor_daemon", "elasticsearch_index")
        elasticsearch_hosts = configs.get("harbor_daemon", "elasticsearch_hosts")
        elasticsearch_username = configs.get("harbor_daemon", "elasticsearch_username")
        elasticsearch_password = str(base64.b64decode(configs.get("harbor_daemon", "elasticsearch_password")), encoding='utf-8')
        crontab = configs.get("harbor_daemon", "crontab")
    else:
        logger.error(cfg['msg'])
        sys.exit(5)

    # 初始化Harbor类
    hab = Harbor(url=harbor_addr, username=harbor_username, password=harbor_password,
                 project=harbor_projects, version=harbor_api_version)
    # 初始化一个elasticsearch类,并判断索引是否存在
    es_client = ElasticsearchClient(elasticsearch_index=elasticsearch_index, elasticsearch_hosts=elasticsearch_hosts,
                                    elasticsearch_username=elasticsearch_username,
                                    elasticsearch_password=elasticsearch_password)
    # 初始化Cluster类
    cluster = Cluster(namespace=docker_daemon_namespace, labels=docker_daemon_label.split(":"))
    # 创建一个线程池
    the_pools = ThreadPoolExecutor(max_workers=int(thread_pools))

    # 定义一个执行函数，用于计划任务执行完成功能
    def executer_scheduler_job(_thread_pools):
        global var_time
        images_all_day = {}
        jk_begin_time = var_time
        jk_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        var_time = jk_end_time
        es_jk_threadpool = ThreadPoolExecutor(max_workers=int(_thread_pools))
        status_code, images_all = es_client.elastic_search_images(begin_time=jk_begin_time, end_time=jk_end_time)
        if status_code:
            if len(images_all['hits']['hits']) > 0:
                images_all_pool = images_all['hits']['hits']
                for im in range(len(images_all_pool)):
                    images_all_day[images_all_pool[im]['_id']] = images_all_pool[im]['_source']['image']
                logger.info("The cron job searches for the following images from elasticsearch: %s" % images_all_day)
                jk_status, target_pods = cluster.get_endpoints()
                logger.info("Get target client endpoints: %s" % target_pods)
                if jk_status:
                    try:
                        for host in target_pods:
                            if check_connection(host, 8000):  # 判断是否可连通
                                jk_thread_name = es_jk_threadpool.submit(post_images_info, host, images_all_day)
                                jk_thread_name.add_done_callback(result_result2)
                    except Exception as e_a:
                        logger.error(e_a)
                else:
                    logger.error(target_pods)
        es_jk_threadpool.shutdown(wait=True)

    # 计划任务格式判断
    cron = crontab.split()
    if len(cron) != 5:
        logger.error("Crontab missing parameters: %s" % crontab)
    else:
        jk_minute = cron[0]
        jk_hour = cron[1]
        jk_day_of_month = cron[2]
        jk_month = cron[3]
        jk_day_of_week = cron[4]
        # 创建一个后台定时器
        try:
            jk_scheduler.add_job(executer_scheduler_job, 'cron', minute=jk_minute, hour=jk_hour, day=jk_day_of_month, month=jk_month, day_of_week=jk_day_of_week, second="0", args=[thread_pools])
            jk_scheduler.start()
        except ValueError as e:
            logger.error(e)

    begin_time = time.time() - 3600
    while True:
        end_time = time.time()
        result = hab.get_project_events(begin_time, end_time)
        if len(result) > 0:
            images_pools = {}
            if harbor_api_version == "v1.0":
                for z in range(len(result)):
                    images_pools[str(result[z]["log_id"])] = ["%s" % utc_gmt(result[z]["op_time"]), "{}/{}:{}".format(harbor_addr, result[z]['repo_name'], result[z]['repo_tag'])]
            else:
                for y in range(len(result)):
                    images_pools[str(result[y]["id"])] = ["%s" % utc_gmt(result[y]["op_time"]), "/".join([harbor_addr, result[y]['resource']])]
            logger.debug(images_pools)
            status, targets = cluster.get_endpoints()  # targets type list
            logger.debug("Get target client endpoints: %s" % targets)
            if status:
                images_pool = {}
                for h in images_pools.keys():
                    images_pool[h] = images_pools[h][1]
                logger.info(images_pool)
                try:
                    for i in targets:
                        if check_connection(i, 8000):  # 判断是否可连通
                            thread_name = the_pools.submit(post_images_info, i, images_pool)
                            thread_name.add_done_callback(result_result)
                except Exception as e:
                    logger.error(e)
                finally:
                    # Write data to ES
                    for es in images_pools.keys():
                        try:
                            es_write = es_client.elastic_write(create_time=images_pools[es][0], image_data=images_pools[es][1])
                        except Exception as e:
                            logger.error(e)
                            logger.error("An exception occurs failed to write %s to ES" % images_pools[es])
                        else:
                            if not es_write:
                                logger.error("Failed to write %s to ES" % images_pools[es])
            else:
                logger.error(targets)
        sleep(listen_interval)
        begin_time = end_time - 300


if __name__ == "__main__":
    var_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 86400))
    project_id = None
    main()
