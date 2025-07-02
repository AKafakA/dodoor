import os
import json
import xml.etree.ElementTree as ET
from collections import OrderedDict
import subprocess

manifest_path = "deploy/resources/configuration/manifest.xml"
template_json_path = "deploy/resources/configuration/host_config_template.json"
host_ssh_path = "deploy/resources/host_addresses/cloud_lab"

if not os.path.exists(host_ssh_path):
    os.makedirs(host_ssh_path)

tree = ET.parse(manifest_path)
num_schedulers = 1
# get root element
nodes = {}
root = tree.getroot()
upload = True


for child in root:
    if "node" in child.tag:
        node_info = {}
        node_name = child.get("client_id")
        nodes[node_name] = node_info
        for subchild in child:
            if "host" in subchild.tag:
                ip_address = subchild.get("ipv4")
                node_info["ip_adresses"] = ip_address
            if "services" in subchild.tag:
                host_name = subchild[0].get("hostname")
                node_info["hostname"] = host_name
            if "hardware_type" in subchild.tag:
                hardware_type = subchild.get("name")
                node_info["hardware_type"] = hardware_type

nodes = OrderedDict(sorted(nodes.items()))

host_files = host_ssh_path + "/test_host"
scheduler_files = host_ssh_path + "/test_scheduler"
node_files = host_ssh_path + "/test_nodes"
node_ip = host_ssh_path + "/test_node_ip"
scheduler_ip = host_ssh_path + "/test_scheduler_ip"

host_config_json = host_ssh_path + "/host_config.json"

host_names = []
host_json = json.load(open(template_json_path, "r"))
with open(host_files, "w+") as f, open(scheduler_files, "w+") as s, open(node_files, "w+") as n, \
        open(node_ip, "w+") as nip, open(scheduler_ip, "w+") as sip:
    j = 0
    for node in nodes:
        node_info = nodes[node]
        f.write("asdwb@" + node_info["hostname"] + "\n")
        if j < num_schedulers:
            s.write("asdwb@" + node_info["hostname"] + "\n")
            sip.write(node + ":" + node_info["ip_adresses"] + "\n")
            j += 1
        else:
            n.write("asdwb@" + node_info["hostname"] + "\n")
            nip.write(node + ":" + node_info["ip_adresses"] + "\n")
        host_names.append("asdwb@" + node_info["hostname"])
        if node_info["hardware_type"] == host_json["scheduler"]["type"]:
            host_json["scheduler"]["hosts"].append(node_info["ip_adresses"])
            host_json["datastore"]["hosts"].append(node_info["ip_adresses"])
        else:
            for node_type in host_json["nodes"]["node.types"]:
                if node_type["node.type"] == node_info["hardware_type"]:
                    node_type["hosts"].append(node_info["ip_adresses"])
                    break

json.dump(host_json, open(host_config_json, "w+"), indent=4)


if upload:
    for host in host_names:
        command = "scp -r %s %s:%s" % (host_ssh_path, host, "~/")
        subprocess.call(command, shell=True)
        print("{} host information updated.".format(host))






