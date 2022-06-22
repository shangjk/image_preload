import requests
import json
import time
import configparser
import base64
import os
from time import sleep
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3 import Retry
import kubernetes.client
from kubernetes.client import ApiException

requests.packages.urllib3.disable_warnings()


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
            print("Exception when calling get %s project_id: %s " % (self.project_name, e))
        else:
            return res.json()[0]['project_id']

    def get_project_events(self, begin_time, end_time):
        begin = time.gmtime(begin_time)
        end = time.gmtime(end_time)
        try:
            if self.version == "v2.0":
                res_events = self.session.get(
                    url=self.url + "projects/{}/logs?q=operation%3Dcreate%2Cop_time%3D%5B{}-{}-{}%20{}%3A{}%3A{}"
                                   "~{}-{}-{}%20{}%3A{}%3A{}%5D".format(self.project_name, begin.tm_year, begin.tm_mon,
                                                                        begin.tm_mday, begin.tm_hour, begin.tm_min,
                                                                        begin.tm_sec, end.tm_year, end.tm_mon,
                                                                        end.tm_mday, end.tm_hour, end.tm_min,
                                                                        end.tm_sec), headers=self.Head)
            elif self.version == "v1.0":
                self.project_id = self.get_project_id()
                res_events = self.session.get(
                    url=self.url + "projects/{}/logs?q=operation%3Dcreate%2Cop_time%3D%5B2022-06-01%2010%3A00%3A00"
                                   "~2022-06-16%2023%3A00%3A00%5D".format(self.project_id), headers=self.Head)
        except Exception as e:
            print("Exception when calling get project %s evetns: %s" % (self.project_name, e))
        else:
            return res_events.json()


class Cluster(object):
    def __init__(self, namespace, labels: list):
        self.token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        self.ca_file = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        self.endpoints_url = "/api/v1/namespaces/{}/endpoints?labelSelector={}%3D{}".format(namespace, labels[0], labels[1])
        self.api_host = "https://kubernetes"
        with open(self.token_file, 'r') as a:
            self.token = a.read()

    def get_endpoints(self):
        Header = {
            ""
        }
        try:
            endpoints = requests.get()







class logging(object):
    def __init__(self, log_dir, info_log, warn_log, error_log):
        self.log_dir = log_dir
        self.info_log = self.log_dir + "/" + info_log
        self.warn_log = self.log_dir + "/" + warn_log
        self.error_log = self.log_dir + "/" + error_log
        if not os.path.exists(self.log_dir):
            os.mkdir(self.log_dir, 755)

    def info(self, msg):
        info_msg = "[{}] [INFO] {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), msg)
        with open(self.info_log, 'a') as info_logfile:
            info_logfile.write(info_msg)
        print(info_msg)

    def warn(self, msg):
        warn_msg = "[{}] [WARN] {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), msg)
        with open(self.warn_log, 'a') as warn_logfile:
            warn_logfile.write(warn_msg)
        print(warn_msg)

    def error(self, msg):
        error_msg = "[{}] [ERROR] {}".format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), msg)
        with open(self.error_log, 'a') as error_logfile:
            error_logfile.write(error_msg)
        print(error_msg)


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
    elif not config.get("harbor_daemon", "listen_interval"):
        return {"res": False, "msg": "{} section harbor_daemon option listen_interval is None !".format(cfg_file)}
    else:
        return {"res": True, "msg": config}


def main():
    # 初始化日志类
    log_dir = "/tmp/"
    info_log = "harbor_api_info.log"
    warn_log = "harbor_api_warn.log"
    error_log = "harbor_api_error.log"
    log_msg = logging(log_dir=log_dir, info_log=info_log, warn_log=warn_log, error_log=error_log)
    # 加载配置文件，并判断必要参数是否为空
    cfg = get_cfg('harbor_api.cfg')
    if cfg['res']:
        configs = cfg['msg']
    else:
        log_msg.error(cfg['msg'])
    # 加载变量
    harbor_addr = configs.get("harbor_info", "harbor_addr")
    harbor_username = configs.get("harbor_info", "harbor_username")
    harbor_password = base64.b64decode(configs.get("harbor_info", "harbor_password"))
    harbor_projects = configs.get("harbor_info", "harbor_projects")
    harbor_api_version = configs.get("harbor_info", "harbor_api_version")
    listen_interval = int(configs.get("harbor_daemon", "listen_interval"))
    # 初始化Harbor类
    hab = Harbor(url=harbor_addr, username=harbor_username, password=harbor_password,
                 project=harbor_projects, version=harbor_api_version)
    begin_time = time.time() - listen_interval
    while True:
        end_time = time.time()
        result = hab.get_project_events(begin_time, end_time)

        print(result)
        sleep(listen_interval)
        print(begin_time, end_time)
        begin_time = end_time


if __name__ == "__main__":
    main()
