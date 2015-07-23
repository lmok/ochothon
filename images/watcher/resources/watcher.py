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

class PublishDictWrapper:

    def __init__(self, watching):
        
        self.publish = {cluster: {} for cluster in watching}

    def insert(self, cluster, name, issue, diagnosis):

        if not name in self.publish[cluster]:
        
            self.publish[cluster][name] = {}

        self.publish[cluster][name][issue] = diagnosis

    def out(self):

        return json.dumps(self.publish)


def watch(remote, watching=['*'], message_log=logger, period=120.0, wait=10.0, checks=3, timeout=20.0):
    """
        Watches a list of clusters for failures in health checks (defined as non-running process status). This fires a number of checks
        every period with a wait between each check. E.g. it can check 3 times every 5-minute period with a 10 second wait between checks.

        Publishes data in the format::

            {

            }

        :param watching: list of glob patterns matching clusters to be watched
        :param period: float amount of seconds in each polling period
        :param wait: float amount of seconds between each check
        :param checks: int number of failed checks allowed before an alert is sent
        :param timeout: float number of seconds allowed for querying ochopod  
    """

    #
    # - Allowed states for subprocess to be in
    #
    allowed = ['running']

    #
    # - Records of previous health checks
    #
    store = {cluster: {'remaining': checks, 'stagnant': False, 'data': {}} for cluster in watching}
    store_indeces = {cluster: {} for cluster in watching}
    store_health = {cluster: {} for cluster in watching}
    
    try:

        while True:

            logger.info('Watcher: polling daycare...')
            #
            # - Dict for publishing JSON data
            #
            publish = PublishDictWrapper(watching)

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

                        logger.warning('Watcher: communication with portal during metrics collection failed.')
                        continue

                    data = json.loads(js['out'])

                    good = sum(1 for key, status in data.iteritems() if status['process'] in allowed)

                    if len(data) == 0:

                        logger.warning('Watcher: did not find any pods under %s.' % cluster)

                    #
                    # - Store current indeces in dict of {key: [list of found indeces]}
                    #
                    curr_indeces = {}

                    #
                    # - Store health in dict of {key: {health: count}}
                    #
                    curr_health = {}

                    for key in data.keys():
                        
                        #
                        # - Some extraneous split/joins in case user has used ' #' in namespace
                        #
                        index = int(key.split(' #')[-1])
                        name = ' #'.join(key.split(' #')[:-1])

                        #
                        # - update current health records with status from grep for this particular namespace/cluster 
                        #
                        curr_indeces[name] = [index] if name not in curr_indeces else curr_indeces[name] + [index]
                        
                        curr_health[name] = {'up': 0, 'down': 0} if not name in curr_health else curr_health[name]

                        if data[key]['process'] in allowed:

                            curr_health[name]['up'] += 1

                        else:

                            curr_health[name]['down'] += 1

                    #
                    # - Analyse pod indeces
                    #
                    for name, indeces in curr_indeces.iteritems():

                        #
                        # - First time cluster has been observed
                        #
                        if not name in store_indeces[cluster]:

                            store_indeces[cluster][name] = indeces
                            continue

                        #
                        # - The base index has changed (not supposed to happen even when scaling to an instance # above 0)
                        # - warn anyway if indeces have jumped even if health is fine
                        #
                        base_index = sorted(store_indeces[cluster][name])[0]

                        if base_index not in indeces:

                            #logger.warning('Previous lowest index for %s (#%d) has changed (now #%d).' % (name, base_index, sorted(indeces)[0]))
                            publish.insert(cluster, name, 'changed_base_index', '#%d to #%d' % (base_index, sorted(indeces)[0]))

                        #
                        # - Some previously-stored indeces have disappeared if delta is not None
                        #
                        delta = set(store_indeces[cluster][name]) - set(indeces)
                        
                        if delta:

                            #logger.warning('Previous indeces for %s (#%s) have been lost.' %(name, ', #'.join(map(str, delta))))
                            publish.insert(cluster, name, 'lost_indeces', '[%s]' % (', '.join(map(str, delta))))

                        #
                        # Update the stored indeces with current list
                        #
                        store_indeces[cluster][name] = indeces

                    #
                    # - Analyse pod health
                    #
                    for name, health in curr_health.iteritems():

                        #
                        # - First time cluster has been observed
                        #
                        if not name in store_health[cluster]:

                            store_health[cluster][name] = {'remaining': checks, 'activity': 'stagnant'}

                        #
                        # - Cluster healthy and stable
                        #
                        elif health['down'] == 0 and health['up'] == store_health[cluster][name]['up']:

                            store_health[cluster][name]['activity'] = 'stable'

                        #
                        # - Cluster healthy but health/count has changed (active)
                        #
                        elif health['down'] == 0:
                            
                            #
                            # - Reset remaining checks
                            #
                            store_health[cluster][name]['remaining'] = checks
                            store_health[cluster][name]['activity'] = 'active'

                        #
                        # - Cluster unhealthy but active
                        #
                        elif health['up'] != store_health[cluster][name]['up'] or health['down'] != store_health[cluster][name]['down']:

                            store_health[cluster][name]['activity'] = 'active'

                        #
                        # - Cluster unhealthy and stagnant
                        #
                        else:

                            store_health[cluster][name]['remaining'] -= 1
                            store_health[cluster][name]['activity'] = 'stagnant'

                        store_health[cluster][name]['down'] = health['down']
                        store_health[cluster][name]['up'] = health['up']

                    #
                    # - Check if any application has disappeared entirely
                    #
                    for name, health in store_health[cluster].iteritems():

                        if name not in curr_health:

                            store_health[cluster][name] = {'remaining': 0, 'activity': 'absent', 'up': 0, 'down': 0}
                            publish.insert(cluster, name, 'lost_indeces', (', '.join(map(str, store_indeces[cluster][name]))))
                            publish.insert(cluster, name, 'changed_base_index', '#%d to None' % (str(sorted(store_indeces[cluster][name])[0])))

                time.sleep(wait)

            #
            # - Check allowance exceeded for each cluster's health; attach to the publisher if
            # - all health checks had failed
            #
            for cluster, stored in store_health.iteritems():
                
                for name, health in stored.iteritems():

                    if 'remaining' in health: #not health['remaining'] > 0:

                        del(health['remaining'])
                        publish.insert(cluster, name, 'health', dict(health))

                    #
                    # - Reset checks count for next period
                    #
                    store_health[cluster][name]['remaining'] = checks

            message_log.info(publish.out())

            time.sleep(period)

    except Exception as failure:

        logger.fatal('Error on line %s' % (sys.exc_info()[-1].tb_lineno))
        logger.fatal('unexpected condition -> %s' % failure)

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
        clusters = env['DAYCARE'].split(',') if 'DAYCARE' in env else ['*'] 

        #
        # - Get the portal that we found during cluster configuration (see pod/pod.py)
        #
        _, lines = shell('cat /opt/watcher/.portal')
        portal = lines[0]
        assert portal, '/opt/watcher/.portal not found (pod not yet configured ?)'
        logger.debug('using proxy @ %s' % portal)
        
        #
        # - Prepare message logging
        #
        from logging import INFO, Formatter
        from logging.config import fileConfig
        from logging.handlers import RotatingFileHandler

        #: the location on disk used for logging watcher messages
        message_file = '/var/log/watcher.log'

        #
        # - load our logging configuration from config/log.cfg
        # - make sure to not reset existing loggers
        #
        fileConfig('/opt/watcher/config/log.cfg')

        #
        # - add a small capacity rotating log
        # - this will be persisted in the container's filesystem and retrieved via /log requests
        # - an IOError here would mean we don't have the permission to write to /var/log for some reason
        #
        message_log = logging.getLogger('watcher')

        try:

            handler = RotatingFileHandler(message_file, maxBytes=32764, backupCount=3)
            handler.setLevel(INFO)
            handler.setFormatter(Formatter('%(message)s'))
            message_log.addHandler(handler)

        except IOError:

            logger.warning('Message logger not enabled')
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
        watch(_remote, clusters, message_log=message_log)        

    except Exception as failure:

        logger.fatal('Error on line %s' % (sys.exc_info()[-1].tb_lineno))
        logger.fatal('unexpected condition -> %s' % failure)

    finally:

        sys.exit(1)
