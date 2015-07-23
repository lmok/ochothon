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
import logging
import json
import os
from ochopod.core.utils import shell
from ochopod.bindings.ec2.marathon import Pod
from ochopod.models.piped import Actor as Piped
from ochopod.models.reactive import Actor as Reactive
from jinja2 import Environment, FileSystemLoader

logger = logging.getLogger('ochopod')

if __name__ == '__main__':

    cfg = json.loads(os.environ['pod'])

    class Model(Reactive):

        depends_on = ['portal']

    class Strategy(Piped):

        cwd = '/opt/watcher'
        pipe_subprocess = True

        def initialize(self):

            splunk = cfg['splunk']

            env = Environment(loader=FileSystemLoader('/opt/watcher/templates'))
            template = env.get_template('props.conf')
            
            with open('/opt/splunkforwarder/etc/system/local/props.conf', 'wb') as f:
                f.write(template.render(
                    {
                        'sourcetype': splunk['sourcetype']
                    }))

            shell('splunk start --accept-license && splunk edit user admin -password foo -auth admin:changeme')
            
            for url in splunk['forward'].split(','):
                shell('splunk add forward-server %s' % url)

            shell('touch /var/log/watcher.log && splunk add monitor /var/log/watcher.log -index service -sourcetype %s' % splunk['sourcetype'])

        def can_configure(self, cluster):

            #
            # - we need one portal pod
            #
            assert len(cluster.dependencies['portal']) == 1, 'need 1 portal'

        def configure(self, cluster):

            #
            # - look the ochothon portal up @ TCP 9000
            # - update the resulting connection string into /opt/watcher.portal 
            # - this will be used by the watcher to poll for health
            #
            with open('/opt/watcher/.portal', 'w') as f:
                f.write(cluster.grep('portal', 9000))

            return 'python -u watcher.py', {}

    Pod().boot(Strategy, model=Model)