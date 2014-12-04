import logging
import socket
from rackattack.ssh import tunnelconnection


class TunnelLocalForwardingServer:
    def __init__(self, openDirectTCPIPChannelCallback, addConnectionCallback, remoteEndpoint):
        self._openDirectTCPIPChannelCallback = openDirectTCPIPChannelCallback
        self._addConnectionCallback = addConnectionCallback
        if isinstance(remoteEndpoint, int):
            self._remoteEndpoint = ('localhost', remoteEndpoint)
        else:
            self._remoteEndpoint = remoteEndpoint
        self._logger = logging.getLogger('ssh')
        self._socket = socket.socket()
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.bind(("", 0))
        self._socket.listen(5)

    def remoteEndpoint(self):
        return self._remoteEndpoint

    def port(self):
        return self._socket.getsockname()[1]

    def done(self):
        return False

    def close(self):
        self._socket.close()
        self._socket = None

    def selectableSockets(self):
        assert self._socket is not None
        return [self._socket]

    def work(self, readReadySockets):
        assert self._socket is not None
        if self._socket not in readReadySockets:
            return
        connection, peer = self._socket.accept()
        try:
            connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            channel = self._openDirectTCPIPChannelCallback(
                remoteEndpoint=self._remoteEndpoint, localEndpoint=peer)
        except:
            self._logger.debug("Unable to connect to remote endpoint %(endpoint)s", dict(
                endpoint=self._remoteEndpoint))
            connection.close()
            return
        tunnelConnection = tunnelconnection.TunnelConnection(socket=connection, channel=channel)
        self._addConnectionCallback(tunnelConnection)
        self._logger.debug("Successfully connected %(localEndpoint)s to %(remoteEndpoint)s", dict(
            localEndpoint=peer, remoteEndpoint=self._remoteEndpoint))
