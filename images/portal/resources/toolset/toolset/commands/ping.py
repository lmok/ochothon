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
import yaml

from toolset.io import fire, run
from toolset.tool import Template
from yaml import YAMLError

#: Our ochopod logger.
logger = logging.getLogger('ochopod')


def go():

    class _Tool(Template):

        help = \
            '''
                Sends a block of arbitrary YAML data to the specified cluster(s). Each container will receive the
                data as a dict and pass it to its signal() callback for processing.

                This tool supports optional output in JSON format for 3rd-party integration via the -j switch.
            '''

        tag = 'ping'

        def customize(self, parser):

            parser.add_argument('yaml', nargs=1, help='YAML file, or verbatim json string if -v')
            parser.add_argument('clusters', type=str, nargs='*', default='*', help='1+ clusters (can be a glob pattern, e.g foo*)')
            parser.add_argument('-j', action='store_true', dest='json', help='json output')
            parser.add_argument('-v', dest='verbatim', action='store_true', help='interpret yaml argument as a json string payload')

        def body(self, args, proxy):

            try:

                payload = {}

                if not args.verbatim:

                    with open(args.yaml[0], 'r') as f:
                        payload = yaml.load(f)

                else:

                    payload = json.loads(args.yaml[0])

                total = 0
                merged = {}
                for token in args.clusters:

                    def _query(zk):
                        replies = fire(zk, token, 'control/signal', js=json.dumps(payload))
                        return len(replies), {key: data for key, (_, data, code) in replies.items() if code == 200}

                    pods, js = run(proxy, _query)
                    merged.update(js)
                    total += pods

                pct = (len(merged) * 100) / total if total else 0
                logger.info(json.dumps(merged) if args.json else '%d%% replies, pinged %d pods' % (pct, len(merged)))

            except IOError:

                logger.info('unable to load %s as yaml (maybe you want -v)' % args.yaml[0])

            except YAMLError as failure:

                if hasattr(failure, 'problem_mark'):
                    mark = failure.problem_mark
                    assert 0, '%s is invalid (line %s, column %s)' % (args.yaml, mark.line+1, mark.column+1)

    return _Tool()