import socket
import logging


class Run:
    def __init__(self, sshClient):
        self._sshClient = sshClient
        self._logger = logging.getLogger('ssh')

    def winScript(self, winScript, outputTimeout=20 * 60):
        self._logger.debug("Running win script:\n\n%(winScript)s\n", dict(winScript=winScript))
        winCommand = " && ".join(winScript.strip().split("\n"))
        winCommand = "( " + winCommand + " ) 2>&1"
        return self._script(winCommand, winScript, outputTimeout=outputTimeout)

    def script(self, bashScript, outputTimeout=20 * 60):
        self._logger.debug("Running bash script:\n\n%(bashScript)s\n", dict(bashScript=bashScript))
        command = "\n".join([
            "sh 2>&1 << 'RACKATTACK_SSH_RUN_SCRIPT_EOF'",
            bashScript,
            "RACKATTACK_SSH_RUN_SCRIPT_EOF\n"])
        return self._script(command, bashScript, outputTimeout=outputTimeout)

    def _script(self, command, script, outputTimeout=20 * 60):
        transport = self._sshClient.get_transport()
        chan = transport.open_session()
        try:
            chan.exec_command(command)
            chan.settimeout(outputTimeout)
            stdin = chan.makefile('wb', -1)
            stdout = chan.makefile('rb', -1)
            stderr = chan.makefile_stderr('rb', -1)
            stdin.close()
            outputArray = []
            try:
                while True:
                    segment = stdout.read(4 * 1024)
                    if segment == "":
                        break
                    outputArray.append(segment)
            except socket.timeout:
                output = "".join(outputArray)
                e = socket.timeout(
                    "Timeout running '%s', no input for timeout of '%s'. Partial output was\n:%s" % (
                        script, outputTimeout, output))
                e.output = output
                raise e
            output = "".join(outputArray)
            status = chan.recv_exit_status()
            stderr.read()
            stdout.close()
            stderr.close()
            self._logger.debug("Script output:\n\n%(output)s\n", dict(output=output))
            if status != 0:
                e = Exception("Failed running '%s', status '%s', output was:\n%s" % (
                    script, status, output))
                e.output = output
                raise e
            return output
        finally:
            chan.close()

    def backgroundScript(self, bashScript):
        command = "\n".join([
            "nohup sh << 'RACKATTACK_SSH_RUN_SCRIPT_EOF' >& /dev/null &",
            bashScript,
            "RACKATTACK_SSH_RUN_SCRIPT_EOF\n"])
        transport = self._sshClient.get_transport()
        chan = transport.open_session()
        try:
            chan.exec_command(command)
            status = chan.recv_exit_status()
            if status != 0:
                raise Exception("Failed running '%s', status '%s'" % (bashScript, status))
        finally:
            chan.close()
