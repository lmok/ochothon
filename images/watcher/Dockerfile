FROM autodeskcloud/pod:1.0.2

#
# - add pip, pyyaml & redis
#
RUN apt-get update && apt-get -y install python-pip vim
RUN pip install --no-use-wheel --upgrade distribute
RUN pip install redis pyyaml

#
# - add our internal toolset package
# - install it
#
ADD resources/toolset /opt/watcher/toolset

#
# - add our spiffy pod script + the watcher code itself
# - add our supervisor script
# - start supervisor
#
ADD resources/pod /opt/watcher/pod
ADD resources/watcher.py /opt/watcher/
ADD resources/supervisor /etc/supervisor/conf.d
CMD /usr/bin/supervisord -n -c /etc/supervisor/supervisord.conf