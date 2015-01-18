import paramiko
import tempfile
import time
import socket
import logging
from rackattack.ssh import ftp
from rackattack.ssh import run
from rackattack.ssh import dirftp
from rackattack.ssh import tunnel


def discardParamikoLogs():
    logging.getLogger('paramiko').setLevel(logging.CRITICAL)


def discardSSHDebugMessages():
    logging.getLogger('ssh').setLevel(logging.INFO)


class Connection:
    def __init__(self, hostname, username, password=None, key=None, port=22, **kwargs):
        assert (key or password) and not (key and password)
        self._hostname = hostname
        self._username = username
        self._password = password
        self._key = key
        self._port = port
        self._sshClient = None
        self._tunnel = None

    @property
    def run(self):
        return run.Run(self._sshClient)

    @property
    def ftp(self):
        return ftp.FTP(self._sshClient)

    @property
    def dirFTP(self):
        return dirftp.DirFTP(self._sshClient)

    @property
    def tunnel(self):
        if self._tunnel is None:
            self._tunnel = tunnel.Tunnel(self._openDirectTCPIPChannelCallback)
        return self._tunnel

    def close(self):
        if self._tunnel is not None:
            self._tunnel.close()
            self._tunnel = None
        if self._sshClient is not None:
            self._sshClient.close()
            self._sshClient = None

    def connect(self):
        if self._password:
            password = dict(password=self._password)
        else:
            keyFile = tempfile.NamedTemporaryFile()
            keyFile.write(self._key)
            keyFile.flush()
            password = dict(key_filename=keyFile.name)

        self._sshClient = paramiko.SSHClient()
        self._sshClient.known_hosts = None
        self._sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._sshClient.connect(
            hostname=self._hostname, port=self._port,
            username=self._username,
            look_for_keys=False, allow_agent=False,
            timeout=10,
            ** password)
        self._sshClient.get_transport().set_keepalive(15)

    def waitForTCPServer(self, timeout=60, interval=0.1):
        before = time.time()
        while time.time() - before < timeout:
            if self._rawTCPConnect((self._hostname, self._port)):
                return
            time.sleep(interval)
        raise Exception("SSH TCP Server '%(hostname)s:%(port)s' did not respond within timeout" % dict(
            hostname=self._hostname, port=self._port))

    def _rawTCPConnect(self, tcpEndpoint):
        s = socket.socket()
        try:
            s.connect(tcpEndpoint)
            return True
        except:
            return False
        finally:
            s.close()

    def _openDirectTCPIPChannelCallback(self, remoteEndpoint, localEndpoint):
        result = self._sshClient.get_transport().open_channel('direct-tcpip', remoteEndpoint, localEndpoint)
        if result is None:
            raise Exception("Remote SSH server rejected local forwarding tunnel to %s", (remoteEndpoint,))
        return result
