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
from os import environ
from os.path import basename, expanduser, isfile
from subprocess import Popen, PIPE
from ochopod.core.fsm import diagnostic

logger = logging.getLogger('ochopod')

def shell(snippet):
    """
        Helper invoking a shell command and returning its stdout broken down by lines as a list. The sub-process
        exit code is also returned. Since it's crucial to see what's going on when troubleshooting Jenkins the
        shell command output is logged line by line.
        :type snippet: str
        :param snippet: shell snippet, e.g "echo foo > /bar"
        :rtype: (int, list) 2-uple
    """

    out = []
    logger.debug('shell> %s' % snippet)
    pid = Popen(snippet, shell=True, stdout=PIPE, stderr=PIPE)
    while pid.poll() is None:
        stdout = pid.stdout.readline()
        out += [stdout]
        line = stdout[:-1]
        if line:
            logger.debug('shell> %s' % (line if len(line) < 80 else '%s...' % line[:77]))

    code = pid.returncode
    return code, out

def output(js, cluster):
    """
        Lightweight helper for logging results from scale requests.
    """

    if not js['ok']:
        logger.warning('Communication with portal failed when trying to scale clusters under %s.' % cluster)

    outs = json.loads(js['out'])
    failed = any(['failed' in scaled for key, scaled in outs.iteritems()])

    if failed:

        import pprint
        logger.warning('Scaling %s failed. Report:\n%s' % (cluster, pprint.pformat(outs)))

    else:

        for name, data in outs.iteritems():

            logger.info('Scaled: %d/%d pods are running under %s' % (data['running'], data['requested'], name))

def autoscale(remote, clusters, period=300.0):
    """
        Scales the cluster automatically.

        This particular example is meant to scale a cluster of pods with this config in its lifecycle::

            from random import choice

            checks = 3
            check_every = 10.0
            pipe_subprocess = True
            metrics = True

            def sanity_check(self, pid):
                
                #
                # - Randomly decide to be stressed  
                #
                return {'stressed': choice(['Very', 'Nope'])}

        General usage for this function:
        :param clusters: list of strings matching particular namespace/clusters for scaling
        :param period: period (secs) to wait before polling for metrics and scaling
    """

    unit = {
        'instances': 1,
    }   

    lim = {
        'instances': 4,
    }

    while True:

        time.sleep(period)
        
        for cluster in clusters:

            #
            # - Retrieve metrics for the namespace/cluster, ignoring the index
            # 
            js = remote('poll %s -j' % cluster)
            
            if not js['ok']:
                logger.warning('Communication with portal during metrics collection failed.')
                continue

            mets = json.loads(js['out'])

            stressed = sum(1 for key, item in mets.iteritems() if item['stressed'] == 'Very')

            #
            # - Scale up/down based on how stressed the cluster is and if resources
            # - are within the limits
            #
            js = {}

            if stressed > len(mets)/2.0 and len(mets) + unit['instances'] <= lim['instances']:

                    js = remote('scale %s -i %d -j' % (cluster, len(mets) + unit['instances']))

            elif stressed < len(mets)/2.0 and len(mets) > unit['instances']:
                    
                    js = remote('scale %s -i %d -j' % (cluster, len(mets) - unit['instances']))

            #
            # - Output for calls to scale
            #
            if not js == {}:

                output(js, cluster)

def pulse(remote, clusters, period=300.0):
    """
        Scales cluster up and down periodically
        :param clusters: list of strings matching particular namespace/clusters for scaling
        :param period: period (secs) between pulses
    """

    #
    # - Pulse cluster up and down
    #
    i = 0

    while True:

        time.sleep(period)

        for cluster in clusters:

            cmd = ''

            if i % 4 == 0:

                cmd = 'scale %s -i %d -j' % (cluster, 1)

            elif i % 4 == 1 or i % 4 == 3:

                cmd = 'scale %s -i %d -j' % (cluster, 2)

            else:

                cmd = 'scale %s -i %d -j' % (cluster, 3)
            
            js = remote(cmd)
            output(js, cluster)

            i += 1

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
        # - Check for passed set of scalee clusters in deployment yaml
        #
        clusters = []
        if 'SCALEES' in env:
            clusters = env['SCALEES'].split(',')

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
            snippet = 'curl -X POST -H "X-Shell:%s" %s %s/shell' % (line, ' '.join(files), portal)
            code, lines = shell(snippet)
            assert code is 0, 'i/o failure (is the proxy portal down ?)'
            js = json.loads(lines[0])
            elapsed = time.time() - now
            return js

        # Use our very simple autoscaling routine
        autoscale(_remote, clusters, 30.0)

    except Exception as failure:

        logger.fatal('unexpected condition -> %s' % diagnostic(failure))

    finally:

        sys.exit(1)