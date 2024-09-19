import os
from xml.dom.minidom import parse
import xml.etree.ElementTree as ET
from collections import OrderedDict
import subprocess

manifest_path = "deploy/resources/cl_manifest.xml"
output_path = "deploy/resources/host_addresses/cloud_lab"

if not os.path.exists(output_path):
    os.makedirs(output_path)

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

nodes = OrderedDict(sorted(nodes.items()))

host_files = output_path + "/test_host"
scheduler_files = output_path + "/test_scheduler"
node_files = output_path + "/test_nodes"
node_ip = output_path + "/test_node_ip"
scheduler_ip = output_path + "/test_scheduler_ip"

host_names = []
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

if upload:
    for host in host_names:
        command = "scp -r %s %s:%s" % (output_path, host, "~/")
        subprocess.call(command, shell=True)
        print("{} host information updated.".format(host))






