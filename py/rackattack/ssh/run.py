import socket
import logging


class Run:
    def __init__(self, sshClient):
        self._sshClient = sshClient
        self._logger = logging.getLogger('ssh')

    def script(self, bashScript, verbose=True, outputTimeout=20 * 60):
        try:
            return self.execute(bashScript, outputTimeout, wrapCmd=True, verbose=verbose)
        except Exception as e:
            e.args += ('When running bash script "%s"' % bashScript),
            raise

    def execute(self, command, outputTimeout=20 * 60, wrapCmd=True, verbose=True, connectionTimeout=60):
        transport = self._sshClient.get_transport()
        chan = transport.open_session(timeout=connectionTimeout)
        commandToExecute = self._wrapCommand(command) if wrapCmd else command
        try:
            if verbose:
                self._logger.debug("Running bash script: %(cmd)s" % dict(cmd=command.strip()))
            chan.exec_command(commandToExecute)
            chan.settimeout(outputTimeout)
            stdin = chan.makefile('wb', -1)
            stdout = chan.makefile('rb', -1)
            stderr = chan.makefile_stderr('rb', -1)
            stdin.close()
            output = self._readOutput(stdout, outputTimeout)
            status = chan.recv_exit_status()
            stderr.read()
            stdout.close()
            stderr.close()
            if verbose and output:
                self._logger.debug("SSH Execution output: %(output)s" % dict(output="\n" + output))
            if status != 0:
                e = Exception("Failed executing, status '%s', output was:\n%s" % (status, output))
                e.output = output
                raise e
            return output
        finally:
            chan.close()

    def _wrapCommand(self, command):
        return "\n".join(["sh 2>&1 << 'RACKATTACK_SSH_RUN_SCRIPT_EOF'",
                          command,
                          "RACKATTACK_SSH_RUN_SCRIPT_EOF\n"])

    def _readOutput(self, stdout, outputTimeout):
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
                "Timeout executing, no input for timeout of '%s'. Partial output was\n:%s" % (
                    outputTimeout, output))
            e.output = output
            raise e
        return "".join(outputArray)

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
