import os

host = "10.30.223.31"
user = "josep"
password = "josep"


os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'mkdir -p micro-controllers/docker_daemon'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/swift/', user, host, 'micro-controllers/'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/bin/DockerDaemon.jar', user, host, 'micro-controllers/docker_daemon/'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/lib/SBusJavaFacade.jar', user, host, 'micro-controllers/docker_daemon/'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/lib/spymemcached-2.12.1.jar', user, host, 'micro-controllers/docker_daemon/'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/lib/jedis-2.9.0.jar', user, host, 'micro-controllers/docker_daemon/'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/start_daemon.sh', user, host, 'micro-controllers/docker_daemon/'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/logback.xml', user, host, 'micro-controllers/docker_daemon/'))

print "--> FILES UPLOADED"

#os.system('sshpass -p %s ssh %s@%s "%s" > /dev/null' % (password, user, host, 'sudo josep/copy_middleware.sh'))
os.system('sshpass -p %s ssh %s@%s "%s"' % (password, user, host, 'sudo swift-init main restart > /dev/null'))
print "--> FINISH"
