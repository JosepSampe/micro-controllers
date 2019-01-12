import os

host = "10.30.223.31"
user = "josep"
password = "josep"


os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'mkdir -p micro-controllers'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/swift/*', user, host, 'micro-controllers'))

os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/bin/DockerDaemon.jar', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/lib/SBusJavaFacade.jar', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/lib/spymemcached-2.12.1.jar', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/SBus/SBusJavaFacade/bin/libjsbus.so', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/SBus/SBusTransportLayer/bin/sbus.so', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/lib/jedis-2.9.0.jar', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/start_daemon.sh', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/logback.xml', user, host, '/opt/vertigo'))

print "--> FILES UPLOADED"

os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'sudo cp /opt/vertigo/* /home/docker_device/vertigo/scopes/97936da8b6b54/ > /dev/null'))
os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'sudo chown -R swift:swift /home/docker_device/vertigo/scopes/ > /dev/null'))
os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'sudo swift-init main restart > /dev/null'))
print "--> FINISH"
