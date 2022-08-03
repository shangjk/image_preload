import docker
from requests_unixsocket import Session
from flask import Flask, request, jsonify, Response
import os
import sys
import requests
import socket
import logging
import prometheus_client
import time
from prometheus_client import Gauge
from apscheduler.schedulers.background import BackgroundScheduler

logger = logging.getLogger("preload-client")
# 定义logging日志格式配置
logging.basicConfig(filename="/tmp/docker_api.log", filemode="a", format='%(asctime)s [%(levelname)s] %(lineno)d  %(message)s', level='INFO')

# 定义一个控制台输出的日志器
log_fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(lineno)d  %(message)s')
screen_handler = logging.StreamHandler()
# screen_handler.setLevel("DEBUG")  # 该配置不生效
screen_handler.setFormatter(fmt=log_fmt)
logger.addHandler(screen_handler)

app = Flask(__name__)
images_count_sum = Gauge("images_count_sum", "Number of node images")
if os.path.exists('/var/run/docker.sock'):
    session = Session()
    client = docker.DockerClient(base_url='unix://var/run/docker.sock', max_pool_size=15, timeout=60)
else:
    logging.error("Sock file /var/run/docker.sock is not exists !")
    sys.exit(2)


class Cluster(object):
    def __init__(self):
        self.token_file = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        # self.token_file = "token"
        # self.ca_file = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        self.api_host = "https://192.168.0.1"
        with open(self.token_file, 'r') as a:
            self.token = a.read()
        self.headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "Authorization": "Bearer {}".format(self.token)
        }
        self.status, self.vers = self.get_version()

    def get_version(self):
        version_url = "{}/version".format(self.api_host)
        try:
            res = requests.get(url=version_url, headers=self.headers, verify=False, timeout=5)
        except Exception as e:
            logger.error(e)
            return False, e
        else:
            version = res.json()['gitVersion']
            return True, version

    def get_deployments_images(self):
        if self.status and self.vers == "v1.18.12":
            apis = "apps/v1" if self.vers == "v1.18.12" else "extensions/v1beta1"
        else:
            logger.error("kubernetes version get failed! ")
            apis = "apps/v1"
        deploy_url = "{}/apis/{}/deployments".format(self.api_host, apis)
        images_pool = []
        try:
            res_deploys = requests.get(url=deploy_url, headers=self.headers, verify=False, timeout=20)
        except Exception as e:
            logger.error(e)
            return False, e
        else:
            if res_deploys.status_code == 200:
                try:
                    images_subset = res_deploys.json()['items']
                    if len(images_subset) > 0:
                        for i in images_subset:
                            if "initContainers" in i['spec']["template"]["spec"].keys():
                                for x in i['spec']["template"]["spec"]["initContainers"]:
                                    images_pool.append(x["image"])
                            for y in i['spec']["template"]["spec"]["containers"]:
                                images_pool.append(y["image"])
                        logger.debug("Target images pools : %s" % images_pool)
                except KeyError as e:
                    logger.error(e)
                    return False, e
                else:
                    logger.info(images_pool)
                    return True, images_pool
            else:
                logger.error(res_deploys.text)
                return False, "status_code: {}  messages: {}".format(res_deploys.status_code, res_deploys.text)

    def get_daemonset_images(self):
        if self.status and self.vers == "v1.18.12":
            apis = "apps/v1" if self.vers == "v1.18.12" else "extensions/v1beta1"
        else:
            logger.error("kubernetes version get failed! ")
            apis = "apps/v1"
        daemonset_url = "{}/apis/{}/daemonsets".format(self.api_host, apis)
        ds_images_pool = []
        try:
            res_ds = requests.get(url=daemonset_url, headers=self.headers, verify=False, timeout=20)
        except Exception as e:
            logger.error(e)
            return False, e
        else:
            if res_ds.status_code == 200:
                try:
                    images_subset = res_ds.json()['items']
                    if len(images_subset) > 0:
                        for i in images_subset:
                            if "initContainers" in i['spec']["template"]["spec"].keys():
                                for x in i['spec']["template"]["spec"]["initContainers"]:
                                    ds_images_pool.append(x["image"])
                            for y in i['spec']["template"]["spec"]["containers"]:
                                ds_images_pool.append(y["image"])
                        logger.debug("Target images pools : %s" % ds_images_pool)
                except KeyError as e:
                    logger.error(e)
                    return False, e
                else:
                    logger.info(ds_images_pool)
                    return True, ds_images_pool
            else:
                logger.error(res_ds.text)
                return False, "status_code: {}  messages: {}".format(res_ds.status_code, res_ds.text)

    def get_statefulsets_images(self):
        sts_url = "{}/apis/apps/v1/statefulsets".format(self.api_host)
        sts_images_pool = []
        try:
            res_sts = requests.get(url=sts_url, headers=self.headers, verify=False, timeout=20)
        except Exception as e:
            logger.error(e)
            return False, e
        else:
            if res_sts.status_code == 200:
                try:
                    images_subset = res_sts.json()['items']
                    if len(images_subset) > 0:
                        for i in images_subset:
                            if "initContainers" in i['spec']["template"]["spec"].keys():
                                for x in i['spec']["template"]["spec"]["initContainers"]:
                                    sts_images_pool.append(x["image"])
                            for y in i['spec']["template"]["spec"]["containers"]:
                                sts_images_pool.append(y["image"])
                        logger.debug("Target images pools : %s" % sts_images_pool)
                except KeyError as e:
                    logger.error(e)
                    return False, e
                else:
                    logger.info(sts_images_pool)
                    return True, sts_images_pool
            else:
                logger.error(res_sts.text)
                return False, "status_code: {}  messages: {}".format(res_sts.status_code, res_sts.text)

    def get_jobs_images(self):
        jobs_url = "{}/apis/batch/v1/jobs".format(self.api_host)
        jobs_images_pool = []
        try:
            res_jobs = requests.get(url=jobs_url, headers=self.headers, verify=False, timeout=20)
        except Exception as e:
            logger.error(e)
            return False, e
        else:
            if res_jobs.status_code == 200:
                try:
                    images_subset = res_jobs.json()['items']
                    if len(images_subset) > 0:
                        for i in images_subset:
                            if "initContainers" in i['spec']["template"]["spec"].keys():
                                for x in i['spec']["template"]["spec"]["initContainers"]:
                                    jobs_images_pool.append(x["image"])
                            for y in i['spec']["template"]["spec"]["containers"]:
                                jobs_images_pool.append(y["image"])
                        logger.debug("Target images pools : %s" % jobs_images_pool)
                except KeyError as e:
                    logger.error(e)
                    return False, e
                else:
                    logger.info(jobs_images_pool)
                    return True, jobs_images_pool
            else:
                logger.error(res_jobs.text)
                return False, "status_code: {}  messages: {}".format(res_jobs.status_code, res_jobs.text)

    def get_cronjobs_images(self):
        cronjobs_url = "{}/apis/batch/v1beta1/cronjobs".format(self.api_host)
        cronjobs_images_pool = []
        try:
            res_cronjobs = requests.get(url=cronjobs_url, headers=self.headers, verify=False, timeout=20)
        except Exception as e:
            logger.error(e)
            return False, e
        else:
            if res_cronjobs.status_code == 200:
                try:
                    images_subset = res_cronjobs.json()['items']
                    if len(images_subset) > 0:
                        for i in images_subset:
                            if "initContainers" in i['spec']["template"]["spec"].keys():
                                for x in i['spec']["template"]["spec"]["initContainers"]:
                                    cronjobs_images_pool.append(x["image"])
                            for y in i['spec']["template"]["spec"]["containers"]:
                                cronjobs_images_pool.append(y["image"])
                        logger.debug("Target images pools : %s" % cronjobs_images_pool)
                except KeyError as e:
                    logger.error(e)
                    return False, e
                else:
                    logger.info(cronjobs_images_pool)
                    return True, cronjobs_images_pool
            else:
                logger.error(res_cronjobs.text)
                return False, "status_code: {}  messages: {}".format(res_cronjobs.status_code, res_cronjobs.text)



def return_method_error():
    return "Request method is error! ", 405



@app.route('/pull', methods=['POST'])
def pull_images():
    if request.method != "POST":
        return_method_error
    json_data = request.get_json()
    logger.info("GET Server push json data: %s" % json_data)
    num_fail = 0
    num_success = 0
    num_all = 0
    list_fail = []
    for serial_num in json_data.keys():
        num_all += 1
        image = json_data[serial_num].split(":")
        logger.debug("Ready pull image of list: %s" % image)
        try:
            client.images.pull(repository=image[0], tag=image[1])
        except Exception as e:
            num_fail += 1
            list_fail.append(json_data[serial_num])
            logger.error(e)
        else:
            num_success += 1
    local_ip = socket.gethostbyname(socket.gethostname())
    message = "Preload_client: {}, image total: {}, image pull succeeded: {}, image pull failed: {}, list of failed: {}".format(local_ip, num_all, num_success, num_fail, list_fail)
    if num_success == 0:
        logger.error(message)
    else:
        logger.info(message)
    return message, 200


@app.route('/health', methods=['GET'])
def health():
    if request.method != "GET":
        return_method_error
    try:
        os.path.exists('/var/run/docker.sock')
        os.access('/var/run/docker.sock', os.R_OK)
        client.version()
    except Exception as e:
        return "False", 503
    else:
        return "Good", 200


@app.route('/metrics', methods=['GET'])
def metric():
    if request.method != "GET":
        return_method_error
    # images = client.
    images = session.get('http+unix://%2Fvar%2Frun%2Fdocker.sock/images/json')
    images_count_sum.set(len(images.json()))
    return Response(prometheus_client.generate_latest(images_count_sum), mimetype="text/plain")


def clean_useless_images():
    k8s = Cluster()
    using_images_all = ["registry.cn-shanghai.aliyuncs.com/jacke/pause:3.2"]
    remove_images = []
    dep_status, dep_images = k8s.get_deployments_images()
    if dep_status:
        using_images_all = using_images_all + dep_images
    else:
        logger.error(dep_images)
    ds_status, ds_images = k8s.get_daemonset_images()
    if ds_status:
        using_images_all = using_images_all + ds_images
    else:
        logger.error(ds_images)
    sts_status, sts_images = k8s.get_statefulsets_images()
    if sts_status:
        using_images_all = using_images_all + sts_images
    else:
        logger.error(sts_images)
    logger.info("Use images online: %s" % using_images_all)
    jobs_status, jobs_images = k8s.get_jobs_images()
    if jobs_status:
        using_images_all = using_images_all + jobs_images
    else:
        logger.error(jobs_images)
    cronjobs_status, cronjobs_images = k8s.get_cronjobs_images()
    if cronjobs_status:
        using_images_all = using_images_all + cronjobs_images
    else:
        logger.error(cronjobs_images)

    try:
        untag_images_client = session.post('http+unix://%2Fvar%2Frun%2Fdocker.sock/images/prune?dangling=true')
        node_images = session.get('http+unix://%2Fvar%2Frun%2Fdocker.sock/images/json')
    except Exception as e:
        logger.error(e)
    else:
        if untag_images_client.status_code != 200:
            logger.error("Clean Untagged images failed, status code: {}, failed mesg: {}".format(untag_images_client.status_code, untag_images_client.text))
        if node_images.status_code == 200:
            for img in node_images.json() and len(node_images.json()) > 0:
                if img['RepoTags'] is not None:
                    for y in img['RepoTags']:
                        if y is not None and y not in using_images_all and img['Created'] < int(time.time() - 604800):
                            remove_images.append(y)
            logger.info("Remove images list: %s" % remove_images)
        else:
            logger.error("Status code: {}, messages: {}".format(node_images.status_code, node_images.text))
    finally:
        if len(remove_images) > 0:
            try:
                for i in remove_images:
                    client.images.remove(i)
            except Exception as e:
                logger.error(e)


if __name__ == "__main__":
    jk_scheduler = BackgroundScheduler()
    try:
        jk_scheduler.add_job(clean_useless_images, 'cron', minute=0, hour=1, day="*", month="*", day_of_week="*", second="0")
        jk_scheduler.start()
    except ValueError as e:
        logger.error(e)
        sys.exit(10)
    app.run(port="8000", host="0.0.0.0")
