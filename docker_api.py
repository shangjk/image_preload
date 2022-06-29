import docker
from flask import Flask, request, jsonify, Response
import os
import sys
import socket
import logging
import prometheus_client
from prometheus_client import Gauge

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
    client = docker.DockerClient(base_url='unix://var/run/docker.sock', max_pool_size=10, timeout=60)
else:
    logging.error("Sock file /var/run/docker.sock is not exists !")
    sys.exit(2)


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
    images = client.images.list()
    images_count_sum.set(len(images))
    return Response(prometheus_client.generate_latest(images_count_sum), mimetype="text/plain")


if __name__ == "__main__":
    app.run(port="8000", host="0.0.0.0")
