FROM autodeskcloud/pod:1.0.2

#
# - add pip, pyyaml & redis
#
RUN apt-get update && apt-get -y install python-pip vim
RUN pip install --no-use-wheel --upgrade distribute
RUN pip install redis pyyaml Jinja2

#
# - download the splunk forwarder (6.2.3, build #264376)
# - install it
#
RUN curl http://download.splunk.com/products/splunk/releases/6.2.3/universalforwarder/linux/splunkforwarder-6.2.3-264376-linux-2.6-amd64.deb -o splunk.deb
RUN dpkg -i splunk.deb
RUN ln -s /opt/splunkforwarder/bin/splunk /usr/local/bin
RUN rm splunk.deb

#
# - add our spiffy pod script + the watcher code itself
# - add our supervisor script
# - start supervisor
#
ADD resources/pod /opt/watcher/pod
ADD resources/watcher.py /opt/watcher/
ADD resources/supervisor /etc/supervisor/conf.d
ADD resources/config /opt/watcher/config
ADD resources/templates /opt/watcher/templates
CMD /usr/bin/supervisord -n -c /etc/supervisor/supervisord.conf