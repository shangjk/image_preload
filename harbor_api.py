import sys
import requests
import time
import json
import configparser
import threading
import base64
import os
import logging
import socket
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3 import Retry

requests.packages.urllib3.disable_warnings()


logger = logging.getLogger("preload-server")
# 定义logging日志格式配置
log_fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(lineno)d  %(message)s')
logging.basicConfig(filename="/tmp/harbor_api.log", filemode="a", format='%(asctime)s [%(levelname)s] %(lineno)d  %(message)s', level="INFO")

# 定义一个控制台输出的日志器
screen_handler = logging.StreamHandler()
# screen_handler.setLevel("DEBUG") # 该配置不生效
screen_handler.setFormatter(fmt=log_fmt)
logger.addHandler(screen_handler)


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
        self.project_id = None
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
            self.url = "http://%s" % url + "/api/v1.0/"

    def get_project_id(self):
        try:
            res = self.session.get(url=self.url + "projects?name=%s" % self.project_name, headers=self.Head)
        except Exception as e:
            logger.error("Exception when calling get %s project_id: %s " % (self.project_name, e))
            return False, None
        else:
            logger.debug(res.json())
            return True, res.json()[0]['project_id']

    def get_project_events(self, begin_time, end_time):
        res_events = []
        begin = time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime(begin_time)).split("-")
        end = time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime(end_time)).split("-")
        try:
            if self.version == "v2.0":
                res_url = self.url + "projects/{}/logs?q=operation%3Dcreate%2Cop_time%3D%5B{}-{}-{}%20{}%3A{}%3A{}~{}-{}-{}%20{}%3A{}%3A{}%5D".format(self.project_name, begin[0], begin[1], begin[2], begin[3], begin[4], begin[5], end[0], end[1], end[2], end[3], end[4], end[5])
                res_events = self.session.get(url=res_url, headers=self.Head, timeout=5)
            elif self.version == "v1.0":
                status, self.project_id = self.get_project_id()
                if status:
                    res_url = self.url + "projects/{}/logs?q=operation%3Dcreate%2Cop_time%3D%5B2022-06-01%2010%3A00%3A00~2022-06-16%2023%3A00%3A00%5D".format(self.project_id, begin[0], begin[1], begin[2], begin[3], begin[4], begin[5], end[0], end[1], end[2], end[3], end[4], end[5])
                    res_events = self.session.get(url=res_url, headers=self.Head, timeout=5)
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
                ip_subset = endpoints.json()['items'][0]['subsets'][0]['addresses']
                for i in ip_subset:
                    target_ip_pool.append(i['ip'])
                logger.debug("Target endpoints pools : %s" % target_ip_pool)
                return True, target_ip_pool
            else:
                logger.error(endpoints.text)
                return False, "status_code: {}  messages: {}".format(endpoints.status_code, endpoints.text)


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
    else:
        return {"res": True, "msg": config}


def post_images_info(ip_addr, images:dict):
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
        logger.debug("Exception when check connection %s %s : " % (ip, port, e))
        return False


def main():
    # 定义一个获取线程返回结果变量
    def result_result(future):
        results = future.result()
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
    else:
        logger.error(cfg['msg'])
        sys.exit(5)

    # 初始化Harbor类
    hab = Harbor(url=harbor_addr, username=harbor_username, password=harbor_password,
                 project=harbor_projects, version=harbor_api_version)
    # 初始化Cluster类
    cluster = Cluster(namespace=docker_daemon_namespace, labels=docker_daemon_label.split(":"))
    # 创建一个线程池
    the_pools = ThreadPoolExecutor(max_workers=int(thread_pools))
    begin_time = time.time() - listen_interval
    while True:
        end_time = time.time()
        result = hab.get_project_events(begin_time, end_time)
        if len(result) > 0:
            images_pools = {}
            for y in range(len(result)):
                images_pools[str(y)] = "/".join([harbor_addr, result[y]['resource']])
            logger.debug(images_pools)
            status, targets = cluster.get_endpoints()  # targets type list
            logger.debug("Get target client endpoints: %s" % targets)
            if status:
                try:
                    for i in targets:
                        if check_connection(i, 8000):  # 判断是否可连通
                            thread_name = the_pools.submit(post_images_info, i, images_pools)
                            thread_name.add_done_callback(result_result)
                except Exception as e:
                    logger.error(e)
            else:
                logger.error(targets)
        sleep(listen_interval)
        begin_time = end_time


if __name__ == "__main__":
    main()