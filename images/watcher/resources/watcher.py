#
# Copyright (c) 2015 Autodesk Inc.
# All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import json
import logging
import sys
import time
import pprint
from os import environ
from os.path import basename, expanduser, isfile
from ochopod.core.fsm import diagnostic, shutdown
from ochopod.core.utils import retry, shell

logger = logging.getLogger('ochopod')

def alert(cluster, js, checks):
    
    #
    # - Just warn that health checks have failed for now
    #
    message = 'Clusters under %s: %s health checks FAILED.\n-----Process status-----:\n%s' % (cluster, checks, pprint.pformat(js))
    logger.warning(message)

def reassure(cluster, js, checks):

    #
    # - Notify that health has been restored
    #
    message = 'Clusters under %s: changed status; health OK.\n-----Process status-----:\n%s' % (cluster, pprint.pformat(js))
    logger.info(message)

def watch(remote, watching=['*'], period=300.0, wait=10.0, checks=3, timeout=20.0):
    """
        Watches a list of clusters for failures in health checks (defined as non-running process status). This fires a number of checks
        every period with a wait between each check. E.g. it can check 3 times every 5-minute period with a 10 second wait between checks.

        :param watching: list of glob patterns matching clusters to be watched
        :param period: float amount of seconds in each polling period
        :param wait: float amount of seconds between each check
        :param checks: int number of failed checks allowed before an alert is sent
        :param timeout: float number of seconds allowed for querying ochopod  
    """

    allowed = ['running']

    store = {cluster: [checks, {}] for cluster in watching}

    while True:

        proxy = ZK.start([node for node in environ['OCHOPOD_ZK'].split(',')])

        try:

            #
            # - Poll clusters every period and log consecutive health check failures
            #
            for i in range(checks+1):

                for cluster in watching:

                    #
                    # - Poll health of pod's subprocess
                    #
                    def _query(zk):
                        replies = fire(zk, cluster, 'info')
                        return len(replies), {key: (hints['process'] if code == 200 else code) for key, (index, hints, code) in replies.items()}

                    length, js = run(proxy, _query, timeout)
                    good = sum(1 for key, process in js.iteritems() if str(process) in allowed)

                    #
                    # - Health of cluster is fine: reset stored health check
                    #
                    if length == good:

                        if store[cluster][1] != js:

                            reassure(cluster, js, checks)

                        store[cluster] = [checks, js]

                    #
                    # - Cluster not in good health and has not changed since last check: decrease check allowance
                    #
                    elif store[cluster][1] == js:

                        store[cluster] = [store[cluster][0] - 1, js]

                    #
                    # - Cluster not in good health but status has changed: update stored health check
                    #
                    else:

                        store[cluster][1] = js

                    #
                    # - Check allowance exceeded; send warning message
                    #
                    if not store[cluster][0] > 0:

                        alert(cluster, js, checks)

                time.sleep(wait)

            time.sleep(period)

        except Exception as e:

            raise e

        finally:

            shutdown(proxy)

if __name__ == '__main__':

    try:

        #
        # - parse our ochopod hints
        # - enable CLI logging
        # - pass down the ZK ensemble coordinate
        #
        env = environ
        hints = json.loads(env['ochopod'])
        env['OCHOPOD_ZK'] = hints['zk']

        #
        # - Check for passed set of clusters to be watched in deployment yaml
        #
        clusters = ['*']
        if 'DAYCARE' in env:
            clusters = env['DAYCARE'].split(',')

        #
        # - Get the portal that we found during cluster configuration (see pod/pod.py)
        #
        _, lines = shell('cat /opt/scaler/.portal')
        portal = lines[0]
        assert portal, '/opt/scaler/.portal not found (pod not yet configured ?)'
        logger.debug('using proxy @ %s' % portal)

        #
        # - Remote for direct communication with the portal
        #
        def _remote(cmdline):

            #
            # - this block is taken from cli.py in ochothon
            # - in debug mode the verbatim response from the portal is dumped on stdout
            #
            now = time.time()
            tokens = cmdline.split(' ')
            files = ['-F %s=@%s' % (basename(token), expanduser(token)) for token in tokens if isfile(expanduser(token))]
            line = ' '.join([basename(token) if isfile(expanduser(token)) else token for token in tokens])
            logger.debug('"%s" -> %s' % (line, portal))
            snippet = 'curl -X POST -H "X-Shell:%s" %s %s/shell' % (line, ' '.join(files), portal)
            code, lines = shell(snippet)
            assert code is 0, 'i/o failure (is the proxy portal down ?)'
            js = json.loads(lines[0])
            elapsed = time.time() - now
            logger.debug('<- %s (took %.2f seconds) ->\n\t%s' % (portal, elapsed, '\n\t'.join(js['out'].split('\n'))))
            return js
        
        # Run the watcher routine
        watch(_remote, clusters)        

    except Exception as failure:

        logger.fatal('Error on line %s' % (sys.exc_info()[-1].tb_lineno))
        logger.fatal('unexpected condition -> %s' % failure)

    finally:

        sys.exit(1)
