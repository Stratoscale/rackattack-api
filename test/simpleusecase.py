import subprocess
from rackattack import clientfactory
from rackattack.ssh import connection
from rackattack import api
import ipdb


hint = 'rootfs-basic'
label = subprocess.check_output([
    "solvent", "printlabel", "--product", "rootfs", "--repositoryBasename",
    "rootfs-basic"]).strip()

client = clientfactory.factory()
try:
    requirement = api.Requirement(imageLabel=label, imageHint=hint)
    info = api.AllocationInfo(user='rackattack-api test', purpose='integration test')
    allocation = client.allocate(dict(it=requirement), info)
    print("Created allocation, waiting for node inauguration")
    allocation.wait(timeout=7 * 60)
    print("Allocation successfull, waiting for ssh")
    try:
        nodes = allocation.nodes()
        assert len(nodes) == 1, nodes
        it = nodes['it']
        ssh = connection.Connection(**it.rootSSHCredentials())
        ssh.waitForTCPServer()
        ssh.connect()
        print("SSH connected")
        ipdb.set_trace()
        echo = ssh.run.script("echo hello")
        assert 'hello' in echo, echo
    finally:
        allocation.free()
finally:
    client.close()
