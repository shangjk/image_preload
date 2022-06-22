import docker
from flask import Flask, request, jsonify, Response
import os
import sys
import prometheus_client
from prometheus_client import Gauge

app = Flask(__name__)
images_count_sum = Gauge("images_count_sum", "Number of node images")
if os.path.exists('/var/run/docker.sock'):
    client = docker.DockerClient(base_url='unix://var/run/docker.sock', max_pool_size=10, timeout=60)
else:
    print("Sock file /var/run/docker.sock is not exists !")
    sys.exit(2)


def return_method_error():
    return "Request method is error! ", 405


@app.route('/pull', methods=['POST'])
def pull_images():
    if request.method != "POST":
        return_method_error
    json_data = request.get_json()
    if 'repository' not in json_data.keys() and 'tag' not in json_data.keys():
        return "Request params is error", 403
    try:
        client.images.pull(repository=json_data['repository'], tag=json_data['tag'])
    except Exception as e:
        return e, 503
    else:
        return "success", 200


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
