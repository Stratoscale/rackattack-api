# Rackattack API
API for provisioning rackattack hosts


```python
from rackattack.ssh import connection

def get_connection(ip, username, password, timeout=5 * 60):
    node = connection.Connection(ip, username, password)
    node.waitForTCPServer(timeout=timeout, interval=60)
    node.connect()
    return node

node = get_connection("192.168.1.1", "root", "password")
date = node.run.script("date")

```