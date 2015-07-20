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
import requests
from os import environ
from os.path import basename, expanduser, isfile
from subprocess import Popen, PIPE
from ochopod.core.fsm import diagnostic
from ochopod.core.utils import retry

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
        Helper for logging results from scale requests.
        :param js: jsonified output returned from a request to the portal using shell().
        :param cluster: the glob pattern used to match a cluster
    """

    if not js['ok']:
        logger.warning('Communication with portal failed when trying to scale clusters under %s.' % cluster)

    outs = json.loads(js['out'])
    failed = any(['failed' in scaled for key, scaled in outs.iteritems()])

    if failed:

        import pprint
        logger.warning('Scaling %s FAILURE. Report:\n%s' % (cluster, pprint.pformat(outs)))

    else:

        for name, data in outs.iteritems():

            logger.info('Scaling %s SUCCESS. %d/%d pods are running under %s' % (cluster, data['running'], data['requested'], name))

def proxyscale(remote, clusters, haproxies, period=300.0, reps=5):
    """
        Scales clusters under provided cluster glob patterns according to their load. This is checked through HAproxy pods.

        This example also uses user-defined metrics; the scalees have threaded Flask servers that keep track of the number of open 
        threaded requests at a /threads endpoint. The sanity_check() metrics are::

            from random import choice
            from ochopod.core.utils import merge, retry

            cwd = '/opt/flask'
            checks = 5
            check_every = 1
            metrics = True

            def sanity_check(self, pid):
                
                #
                # - Randomly decide to be stressed  
                # - Curl to Flask in the subprocess to check number of threaded requests running.
                #
                @retry(timeout=30.0, pause=0)
                def _self_curl():
                    reply = get('http://localhost:9000/threads')
                    code = reply.status_code
                    assert code == 200 or code == 201, 'Self curling failed'
                    return merge({'stressed': choice(['Very', 'Nope'])}, json.loads(reply.text))

                return _self_curl()

        General usage for this function:
        :param remote: function used to pass toolset commands to the portal
        :param clusters: list of glob patterns matching particular namespace/clusters for scaling
        :param haproxies: list of glob patterns matching haproxies corresponding to each scalee cluster
        :param period: period (secs) to wait before polling for metrics and scaling
        :param reps: int number of 1-second poll repetitions to get stats from HAProxy
    """ 

    assert period > reps, "A period of %d seconds doesn't allow for %d x 1 second polling repetitions." % (period, reps)

    #
    # - Unit and limits by and to which instance number is scaled
    #
    unit =  1
    lim =  200

    #
    # - Max and min acceptable session rate (sessions/second -- see HAProxy stats parameters) PER POD
    # - Max and min acceptable response rate (threaded response/second) per POD if using flask samples
    #
    ceiling_sessions = 5
    floor_sessions = 1
    ceiling_threads = 5
    floor_threads = 2

    #
    # - Check stats this many times to get an average of session rate (since HAProxy only uses 1 second intervals)
    # - I.e. 5 repetitions averages session rates 5 times with a 1 sec sleep between
    #
    reps = 5

    #
    # - If pods were recently scaled, sleep for half the time to re-poll cluster status quickly
    #
    recent = False

    while True:

        #
        # - Wait for period minus polling reps
        #
        time.sleep(period - reps if not recent else period/2 - reps)
        
        for i, cluster in enumerate(clusters):

            #
            # - Get haproxy... hardcoded for now
            # - TODO: Ask olivier about json output for port command
            #
            haproxy = haproxies[i]
            url = environ['CHEAT']

            # Average sessions/second rate over reps # of repetitions
            avg_sessions = 0
            # Average number of open threads in Flask servers over reps # of repetitions
            avg_threads = 0
            num = 0

            for i in range(reps):

                time.sleep(1)

                #
                # - Get the stats from HAproxy
                # - This will put the csv-formatted stats for the BACKEND servers into a nice little dict 
                # - Look at the haproxy pod for more info (frontend.cfg and local.cfg) 
                #
                @retry(timeout=30.0, pause=0)
                def _backend():   
                    reply = requests.get('http://%s/;csv' % url, auth=('olivier', 'likeschinesefood'))
                    code = reply.status_code
                    assert code == 200 or code == 201, 'Polling HAProxy failed (HTTP %d)' % code
                    lines = map(lambda x: x.split(','), reply.text.splitlines())
                    return dict(zip(lines[0], filter(lambda x: x[0] == 'local' and x[1] == 'BACKEND', lines)[0]))

                #
                # - Running average
                #
                backend = _backend()
                avg_sessions = avg_sessions + (float(backend['rate']) - avg_sessions)/(i + 1)

                #
                # - Number of pods up
                # - TODO ask olivier for json output for grep command... use poll for now
                #
                js = remote('poll %s -j' % cluster)
                
                if not js['ok']:
                    logger.warning('Communication with portal during metrics collection failed.')
                    continue

                mets = json.loads(js['out'])

                num = sum(1 for key in mets.keys())

                threads = sum(float(item['threads']) for key, item in mets.iteritems() if 'threads' in item)

                avg_threads = avg_threads + (threads - avg_threads)/(i + 1)

            #
            # - Not everything is running yet, try again next time
            #
            if num == 0:

                continue

            logger.info('Scaler gathered metrics for %s --> average session rate: %d, average thread rate: %d' % (cluster, avg_sessions/num, avg_threads/num))

            #
            # - Scale up/down based on how stressed the cluster is and if resources
            # - are within the limits
            #
            js = {}

            if avg_sessions/num > ceiling_sessions and avg_threads/num > ceiling_threads and num + unit <= lim:

                    js = remote('scale %s -i %d -j' % (cluster, len(mets) + unit))
                    recent = True

            elif avg_sessions/num < floor_sessions and avg_threads/num < floor_threads and num > unit:
                    
                    js = remote('scale %s -i %d -j' % (cluster, len(mets) - unit))
                    recent = True

            else:
                    recent = False

            #
            # - Output for calls to scale
            #
            if not js == {}:

                output(js, cluster)

def simplescale(remote, clusters, period=300.0):
    """
        Scales the cluster automatically with a simple routine.

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
        :param remote: function used to pass toolset commands to the portal
        :param clusters: list of strings matching particular namespace/clusters for scaling
        :param period: period (secs) to wait before polling for metrics and scaling
    """

    unit =  1

    lim =  4

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

            if stressed > len(mets)/2.0 and len(mets) + unit <= lim:

                    js = remote('scale %s -i %d -j' % (cluster, len(mets) + unit))

            elif stressed < len(mets)/2.0 and len(mets) > unit:
                    
                    js = remote('scale %s -i %d -j' % (cluster, len(mets) - unit))

            #
            # - Output for calls to scale
            #
            if not js == {}:

                output(js, cluster)

def pulse(remote, clusters, period=300.0):
    """
        Scales cluster up and down periodically
        :param remote: function used to pass toolset commands to the portal
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
        # - Check for passed set of scalee clusters, haproxies, and time period in deployment yaml
        #
        clusters = env['SCALEES'].split(',') if 'SCALEES' in env else []

        haproxies = env['HAPROXIES'].split(',') if 'HAPROXIES' in env else []
            
        period = float(env['PERIOD']) if 'PERIOD' in env else 60

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

        # Use our very simple autoscaling routine
        proxyscale(_remote, clusters, haproxies, period)

    except Exception as failure:

        logger.fatal('unexpected condition -> %s' % diagnostic(failure))

    finally:

        sys.exit(1)