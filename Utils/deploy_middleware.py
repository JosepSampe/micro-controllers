import os

host = "10.30.223.31"
user = "josep"
password = "josep"


os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'mkdir -p swift-vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/swift/*', user, host, 'swift-vertigo'))

os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/runtime/bin/DockerDaemon.jar', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/runtime/lib/SBusJavaFacade.jar', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/runtime/lib/spymemcached-2.12.1.jar', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/runtime/lib/jedis-2.9.0.jar', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/runtime/utils/start_daemon.sh', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/runtime/utils/logback.xml', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/runtime/utils/docker_daemon.config', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/bus/SBusJavaFacade/bin/libjsbus.so', user, host, '/opt/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/bus/SBusTransportLayer/bin/sbus.so', user, host, '/opt/vertigo'))

os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'sudo cp /opt/vertigo/* /home/docker_device/vertigo/scopes/f130e1bfd5744/ > /dev/null'))
os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'sudo sed -i \'/redis_ip=/c\\redis_ip=10.30.223.31\' /home/docker_device/vertigo/scopes/f130e1bfd5744/docker_daemon.config'))
os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'sudo sed -i \'/swift_ip=/c\swift_ip=10.30.223.31\' /home/docker_device/vertigo/scopes/f130e1bfd5744/docker_daemon.config'))
os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'sudo chown -R swift:swift /home/docker_device/vertigo/scopes/ > /dev/null'))

print "--> FILES UPLOADED"

os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'sudo swift-init main restart > /dev/null'))
print "--> FINISH"
