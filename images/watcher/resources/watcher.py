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
import ochopod
import os
import pykka
import sys
import tempfile
import time
import shutil
import pprint

from ochopod.core.fsm import diagnostic
from toolset.io import fire, run, ZK

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

def watch(watching=['*'], period=300.0, timeout=60.0, wait=10.0, checks=3):

    allowed = ['running']

    proxy = ZK.start([node for node in os.environ['OCHOPOD_ZK'].split(',')])

    store = {cluster: [checks, {}] for cluster in watching}

    while True:

        #
        # - Poll clusters every period and log consecutive health check failures
        #
        time.sleep(period)

        for i in range(2*checks):

            time.sleep(wait)

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

                # for key, val in js.iteritems():

                #     if not key in store:
                #         store[key] = val

                #     elif store[key] != val:
                #         delta 
                
                #logger.info(pprint.pformat(js))

if __name__ == '__main__':

    try:

        #
        # - parse our ochopod hints
        # - enable CLI logging
        # - pass down the ZK ensemble coordinate
        #
        env = os.environ
        hints = json.loads(env['ochopod'])
        ochopod.enable_cli_log(debug=hints['debug'] == 'true')
        env['OCHOPOD_ZK'] = hints['zk']

        #
        # - Check for passed set of clusters to be watched in deployment yaml
        #
        clusters = ['*']
        if 'DAYCARE' in env:
            clusters = env['DAYCARE'].split(',')
        
        watch(clusters, 300.0, 5.0, 20.0)        

    except Exception as failure:

        logger.fatal('Error on line %s' % (sys.exc_info()[-1].tb_lineno))
        logger.fatal('unexpected condition -> %s' % failure)

    finally:

        sys.exit(1)
