import os

host = "iostack.urv.cat"
user = "lab144"
password = "astl1a4b4"

os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/swift/*', user, host, 'josep/vertigo'))
os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/bin/DockerDaemon.jar', user, host, 'josep/vertigo/docker_daemon/'))
# os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/lib/SBusJavaFacade.jar', user, host, 'josep/vertigo/docker_daemon/') )
# os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/start_daemon.sh', user, host, 'josep/vertigo/docker_daemon/'))
# os.system('sshpass -p %s scp -r %s %s@%s:%s' % (password, '../Engine/docker_daemon/logback.xml', user, host, 'josep/vertigo/docker_daemon/') )

print "--> FILES UPLOADED"

os.system('sshpass -p %s ssh %s@%s "%s" > /dev/null' % (password, user, host, 'sudo josep/copy_middleware.sh'))

print "--> FINISH"
