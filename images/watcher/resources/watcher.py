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
from pykka import ThreadingActor, ThreadingFuture, Timeout, ActorRegistry
from pykka.exceptions import ActorDeadError

logger = logging.getLogger('ochopod')

class PublishDictWrapper:

    def __init__(self, cluster):
        
        self.cluster = cluster
        self.publish = {cluster: {}}

    def insert(self, name, issue, diagnosis):

        if not name in self.publish[self.cluster]:
        
            self.publish[self.cluster][name] = {}

        self.publish[self.cluster][name][issue] = diagnosis

    def out(self):

        return json.dumps(self.publish[self.cluster])

class Watcher(ThreadingActor):

    def __init__(self, remote, cluster='*', message_log=logger, period=30.0, wait=10.0, checks=3, timeout=20.0):

            super(Watcher, self).__init__() 

            self.remote = remote
            self.cluster= cluster
            self.message_log = message_log
            self.period = period
            self.wait = wait
            self.checks = checks
            self.timeout = timeout

    def on_start(self):

        logger.info('Starting Watcher for %s...' % self.cluster)
        self.actor_ref.tell({'action': 'watch', 'store_indeces': {self.cluster: {}}, 'store_health': {self.cluster: {}}})

    def on_receive(self, msg):

        if 'action' in msg and msg['action'] == 'watch':

            store_indeces, store_health = _watch(self.remote, 
                                                cluster=self.cluster, 
                                                message_log=self.message_log, 
                                                period=self.period, 
                                                wait=self.wait,
                                                checks=self.checks, 
                                                timeout=self.timeout,
                                                store_indeces=msg['store_indeces'],
                                                store_health=msg['store_health'])

            self.actor_ref.tell({'action': 'watch', 'store_indeces': store_indeces, 'store_health': store_health})

    def on_stop(self):

        logger.info('Stopping Watcher actor for %s' % cluster)

def _watch(remote, cluster='*', message_log=logger, period=30.0, wait=10.0, checks=3, timeout=20.0, store_indeces=None, store_health=None):
    """
        Watches a list of clusters for failures in health checks (defined as non-running process status). This fires a number of checks
        every period with a wait between each check. E.g. it can check 3 times every 5-minute period with a 10 second wait between checks.

        Publishes data in the format::

            {
                <cluster_1>:
                {
                    {
                        health: {},
                        indeces: {}
                    }
                }
            }

        :param cluster: glob patterns matching clusters to be watched
        :param period: float amount of seconds in each polling period
        :param wait: float amount of seconds between each check
        :param checks: int number of failed checks allowed before an alert is sent
        :param timeout: float number of seconds allowed for querying ochopod  
    """

    print 'Polling cluster %s...' % cluster

    #
    # - Allowed states for subprocess to be in
    #
    allowed = ['running']

    #
    # - Records of previous health checks
    #
    store_indeces = {cluster: {}} if store_indeces is None else store_indeces
    store_health = {cluster: {}} if store_health is None else store_health

    #
    # - Dict for publishing JSON data
    #
    publish = PublishDictWrapper(cluster)

    #
    # - Poll clusters every period and log consecutive health check failures
    #
    for i in range(checks + 1):

        #
        # - Poll health of pod's subprocess
        #
        js = remote('grep %s -j' % cluster)

        if not js['ok']:

            logger.warning('Watcher: communication with portal during metrics collection failed.')
            continue

        data = json.loads(js['out'])

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

                publish.insert(name, 'index_changed_base', '#%d to #%d' % (base_index, sorted(indeces)[0]))

            #
            # - Some previously-stored indeces have disappeared if delta is not None
            #
            delta = set(store_indeces[cluster][name]) - set(indeces)
            
            if delta:

                publish.insert(name, 'indeces_lost', '[%s]' % (', '.join(map(str, delta))))

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

                store_health[cluster][name] = {
                    'remaining': checks, 
                    'ochopod_cluster_activity': 'active',
                    'report_next_failure': True,
                    'report_recovery': False
                }

            #
            # - Cluster healthy and stable
            #
            elif health['down'] == 0 and health['up'] == store_health[cluster][name]['ochopod_cluster_up']:

                store_health[cluster][name].update({
                    'ochopod_cluster_activity': 'stable',
                })

            #
            # - Cluster healthy but health/count has changed (active)
            #
            elif health['down'] == 0:
                
                #
                # - Reset remaining checks
                #
                store_health[cluster][name].update({
                    'remaining': checks,
                    'ochopod_cluster_activity': 'active',
                })

            #
            # - Cluster unhealthy but active
            #
            elif health['up'] != store_health[cluster][name]['ochopod_cluster_up'] or health['down'] != store_health[cluster][name]['ochopod_cluster_down']:

                store_health[cluster][name].update({
                    'ochopod_cluster_activity': 'fluctuating',
                })

            #
            # - Cluster unhealthy and stagnant
            #
            else:

                store_health[cluster][name]['remaining'] -= 1
                store_health[cluster][name]['ochopod_cluster_activity'] = 'stagnant'

            store_health[cluster][name].update({
                'ochopod_cluster_down': health['down'],
                'ochopod_cluster_up': health['up'],
                'ochopod_diagnostic': {key: status for key, status in data.iteritems() 
                    if not status['process'] in allowed and ' #'.join(key.split(' #')[:-1]) == name}
            })

        #
        # - Check if any application has disappeared entirely
        #
        for name, health in store_health[cluster].iteritems():

            if name not in curr_health:

                store_health[cluster][name] = {
                    'remaining': 0, 
                    'ochopod_cluster_activity': 'absent', 
                    'ochopod_cluster_up': 0, 
                    'ochopod_cluster_down': 0, 
                    'ochopod_diagnostic': 'lost cluster'
                }
                publish.insert(name, 'lost_indeces', (', '.join(map(str, store_indeces[cluster][name]))))
                publish.insert(name, 'changed_base_index', '#%d to None' % (str(sorted(store_indeces[cluster][name])[0])))

        time.sleep(wait)

    #
    # - Check allowance exceeded for each cluster's health; attach to the publisher if
    # - all health checks had failed
    # - Do this whole loop once per _watch() call
    #
    for name, health in store_health[cluster].iteritems():

        #
        # - Cluster is unhealthy and stagnant; report just once
        #
        if 'remaining' in health and not health['remaining'] > 0 and health['ochopod_cluster_activity'] == 'stagnant' and health['report_next_failure']:

            publish.insert(name, 'health', {key: item for key, item in health.iteritems() if key not in ['report_next_failure', 'report_recovery', 'remaining']})

            health['report_next_failure'] = False
            health['report_recovery'] = True

        #
        # - Cluster is unhealthy and fluctuating; keep reporting until stagnation/recovery
        #
        elif health['ochopod_cluster_activity'] == 'fluctuating' and health['report_next_failure']:

            publish.insert(name, 'health', {key: item for key, item in health.iteritems() if key not in ['report_next_failure', 'report_recovery', 'remaining']})

            health['report_next_failure'] = True
            health['report_recovery'] = True

        #
        # - Cluster was unhealthy but has recovered; report just once
        #
        elif health['ochopod_cluster_activity'] in ['active', 'stable'] and health['report_recovery']:

            publish.insert(name, 'health', {key: item for key, item in health.iteritems() if key not in ['report_next_failure', 'report_recovery', 'remaining', 'ochopod_diagnostic']})

            health['report_next_failure'] = True
            health['report_recovery'] = False         

        #
        # - Reset checks count for next period
        #
        store_health[cluster][name]['remaining'] = checks

    outs = publish.out()

    if outs != '{}':

        message_log.info(outs)

    time.sleep(period)

    return store_indeces, store_health

if __name__ == '__main__':

    watchers = []

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
        watching = env['DAYCARE'].split(',') if 'DAYCARE' in env else ['*'] 

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
        #
        # - the location on disk used for logging watcher messages
        #
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
        
        #
        # - Check for overlapping clusters matching all glob patterns
        #    
        clusters = []

        for cluster in watching:

            js = _remote('grep %s -j' % cluster)

            if not js['ok']:

                logger.warning('Watcher: communication with portal during initialisation failed.')

            data = json.loads(js['out'])

            clusters += ['%s*' % ' #'.join(key.split(' #')[:-1]) for key in data.keys()]

        clusters = list(set(clusters))

        #
        # - Initialise the watcher actors and start
        #
        watchers = [[Watcher(_remote, cluster, message_log=message_log), cluster] for cluster in clusters]
        refs = [watcher.start(_remote, cluster, message_log=message_log) for watcher, cluster in watchers]

    except Exception as failure:

        logger.fatal('Error on line %s' % (sys.exc_info()[-1].tb_lineno))
        logger.fatal('unexpected condition -> %s' % failure)

    finally:

        for watcher in watchers:

            try:
                
                watcher.stop()

            except Exception as e:

                pass

        sys.exit(1)
