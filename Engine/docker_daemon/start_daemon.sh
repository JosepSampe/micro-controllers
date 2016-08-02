echo 127.0.0.1  $HOSTNAME > /etc/hosts
cp /home/swift/logback.xml /opt/storlets/logback.xml
export CLASSPATH=/opt/storlets/:/opt/storlets/logback-classic-1.1.2.jar:/opt/storlets/logback-core-1.1.2.jar:/opt/storlets/slf4j-api-1.7.7.jar:/opt/storlets/json_simple-1.1.jar:/home/swift/SBusJavaFacade.jar:/home/swift/CDaemon.jar
export LD_LIBRARY_PATH=/opt/storlets
/usr/bin/java com.urv.vertigo.daemon.ContainerDaemon /mnt/channels/vertigo_pipe /mnt/channels/internal_client_pipe TRACE 5 $HOSTNAME
