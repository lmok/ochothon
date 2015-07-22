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

def watch(remote, watching=['*'], period=120.0, wait=10.0, checks=3, timeout=20.0):
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

    store = {cluster: {'remaining': checks, 'stagnant': False, 'data': {}} for cluster in watching}
    store_indeces = {cluster: {} for cluster in watching}
    
    try:

        while True:

            #
            # - Poll clusters every period and log consecutive health check failures
            #
            for i in range(checks + 1):

                for cluster in watching:

                    #
                    # - Poll health of pod's subprocess
                    #
                    js = remote('grep %s -j' % cluster)

                    if not js['ok']:

                        logger.warning('Communication with portal during metrics collection failed.')
                        continue

                    data = json.loads(js['out'])

                    good = sum(1 for key, status in data.iteritems() if status['process'] in allowed)

                    if len(data) == 0:

                        logger.warning('Did not find any pods under %s.' % cluster)
                        continue
                    #
                    # - Check indeces of pods; warn anyway if indeces have jumped even if health is fine
                    # - store current indeces in dict of key: [list of found indeces]
                    #
                    curr_indeces = {}

                    for key in data.keys():
                        
                        #
                        # - Some extraneous split/joins in case user has used ' #' in namespace
                        #
                        index = int(key.split(' #')[-1])
                        name = ' #'.join(key.split(' #')[:-1])

                        curr_indeces[name] = [index] if name not in curr_indeces else curr_indeces[name] + [index]

                    for key, indeces in curr_indeces.iteritems():

                        #
                        # - First time cluster has been observed
                        #
                        if not key in store_indeces[cluster]:

                            store_indeces[cluster][key] = indeces
                            continue

                        #
                        # - The base index has changed (not supposed to happen even when scaling to an instance # > 0)
                        #
                        base_index = sorted(store_indeces[cluster][key])[0]

                        if base_index not in indeces:

                            logger.warning('Previous lowest index for %s (#%d) has changed (now #%d).' % (key, base_index, sorted(indeces)[0]))

                        #
                        # - Some previously-stored indeces have disappeared
                        #
                        delta = set(store_indeces[cluster][key]) - set(indeces)
                        
                        if delta:

                            logger.warning('Previous indeces for %s (#%s) have been lost.' %(key, ', #'.join(map(str, delta))))

                        #
                        # Update the stored indeces with current list
                        #
                        store_indeces[cluster][key] = indeces

                    #
                    # - Health of cluster is fine: reset stored health check count and data
                    #
                    if len(data) == good:

                        if store[cluster]['data'] != data:

                            reassure(cluster, data, checks)

                        store[cluster]['data'] = data
                        store[cluster]['remaining'] = checks

                    #
                    # - Cluster not in good health and has not changed since last check: decrease check allowance
                    #
                    elif store[cluster]['data'] == data:

                        store[cluster]['remaining'] -= 1
                        store[cluster]['stagnant'] = True

                    #
                    # - Cluster not in good health but status has changed: update stored health check
                    #
                    else:

                        store[cluster]['data'] = data
                        store[cluster]['stagnant'] = False

                time.sleep(wait)

            #
            # - Check allowance exceeded for each cluster; send warning messages
            #
            for cluster, stored in store.iteritems():
                
                if not stored['remaining'] > 0:

                    alert(cluster, stored['data'], checks)

                #
                # - Reset checks count for next period
                #
                store[cluster]['remaining'] = checks

            time.sleep(period)

    except Exception as failure:

        logger.fatal('Error on line %s' % (sys.exc_info()[-1].tb_lineno))
        logger.fatal('unexpected condition -> %s' % failure)

    finally:

        sys.exit(1)

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
        _, lines = shell('cat /opt/watcher/.portal')
        portal = lines[0]
        assert portal, '/opt/watcher/.portal not found (pod not yet configured ?)'
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
