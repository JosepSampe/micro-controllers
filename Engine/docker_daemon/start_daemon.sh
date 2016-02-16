echo 127.0.0.1  $HOSTNAME > /etc/hosts
cp /home/swift/logback.xml /opt/ibm/logback.xml
export CLASSPATH=/opt/ibm/:/opt/ibm/logback-classic-1.1.2.jar:/opt/ibm/logback-core-1.1.2.jar:/opt/ibm/slf4j-api-1.7.7.jar:/opt/ibm/json_simple-1.1.jar:/opt/ibm/SBusJavaFacade.jar:/home/swift/CDaemon.jar
export LD_LIBRARY_PATH=/opt/ibm
/usr/bin/java com.urv.controller.daemon.ContainerDaemon /mnt/channels/controller_pipe /mnt/channels/internal_client_pipe TRACE 5 $HOSTNAME
