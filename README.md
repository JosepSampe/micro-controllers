# Micro-controllers
Micro-controllers enable tenants to manage the singularities of the objects in a flexible way. Thus, a micro-controller is a data behavior or management policy that is associated to one or more objects, and to one or more storage operations.
Micro-controllers act as object wrappers that control objects behavior. It is for this reason that we introduce the concept of **smart objects** in cloud object stores.
Micro-controllers follow the active storage approach, that is, they are executed close to the data within the storage infrastructure, but in a sandboxed environemnt by using Dockers.

A micro-controller is a piece of code (Java) that can manage the associated objects. Thus, the programmability that micro-controllers provide allows tenants to create advanced forms of object control. There are two different types of micro-controllers: **synchronous** and **asynchronous**. A synchronous micro-controller blocks the incoming or outcoming request until it completes the execution. In the synchronous model, micro-controllers are executed in real-time, which means that the lifecycle of objects is intercepted. In contrast, asynchronous micro-controllers are event-driven, that is, they run when an event occurs. The request that triggered the event does not wait for their completion.

## Framework
This implementation of micro-controllers is designed for OpenStack Swift. This framework is called **Vertigo**, and it includes an interception middleware for Swift, and an execution engine that puts into execution the micro-controllers in a sand-boxed environment.

## Installation

### All-In-One Machine
For testing purposes, it is possible to install an All-In-One (AiO) machine with all the Crystal components and requirements.
We prepared a script for automating this task. The requirements of the machine are a clean installation of **Ubuntu Server 16.04**, at least **2GB** of RAM, and a **fixed IP address**. It is preferable to upgrade the system to the latest versions of the packages with `apt update && apt dist-upgrade` before starting the installation, and set the server name as `controller` in the `/etc/hostname` file. Then, download the `aio_installation.sh` script and run it as sudo:

```bash
curl -fsSL https://git.io/fhn5r | sudo bash /dev/stdin install
```

The script first installs Keystone, Swift and Horizon (Pike release), then it proceeds to install the micor-controllers framework package (Vertigo). Note that the script uses weak passwords for the installed services, so if you want more secure services, please change them at the top of the script.

By default, the script has low verbosity. To see the full installation log, run the following command in another terminal:

```bash
tail -f /tmp/vertigo_aio_installation.log
```

The script takes long to complete (~10 minutes) (it depends of the network connection). Once completed, you can access to the dashboard by typing the following URL in the web browser: `http://<node-ip>/horizon`.

If you already ran the installation script, you can update micro-controllers framework from this repository by the following command:
```bash
curl -fsSL https://git.io/fhn5r | sudo bash /dev/stdin update
```

## Verify
### Test Swift
To verify the correct operation of the Swift installation, follow these steps:

1- Load credentials:
```bash
source vertigo-openrc
```

2- Create data bucket:
```bash
swift post data
```

3- Create new .json file and upload it to data bucket:
```bash
vi test.json
swift upload data test.json
```

4- Test if you can download the .json file:
```bash
swift download data test.json
or
curl -H "X-Auth-Token:$TOKEN" $STORAGE_URL/data/test.json
```

### Test Storlets
1- Download the .json file, running the No-operation Storlet:
```bash
swift download data test.json -H "X-Run-Storlet:noop-1.0.jar"
```

2- Download the .json file, running the Compression Storlet:
```bash
curl -H "X-Auth-Token:$TOKEN" $STORAGE_URL/data/test.json -H "X-Run-Storlet:compress-1.0.jar" -o test.gz
gunzip test.gz
```

### Test Micro-controllers
1- Assign the No-operation micro-controller to the .json file upon GET requests:
```bash
curl -H "X-Auth-Token:$TOKEN" $STORAGE_URL/data/test.json -X POST -H "X-Vertigo-onGet:noop-1.0.jar"
```

2- Download the .json file that will put into execution the micro-controller:
```bash
swift download data test.json
or
curl -H "X-Auth-Token:$TOKEN" $STORAGE_URL/data/test.json
```

3- Delete the No-operation micro-controller:
```bash
curl -H "X-Auth-Token:$TOKEN" $STORAGE_URL/data/test.json -X POST -H "X-Vertigo-onGet-Delete:noop-1.0.jar"
```

4- Assign the Counter micro-controller to the .json file upon GET requests:
```bash
curl -H "X-Auth-Token:$TOKEN" $STORAGE_URL/data/test.json -X POST -H "X-Vertigo-onGet:counter-1.0.jar"
```

5- Download the .json file that will put into execution the micro-controller:
```bash
swift download data test.json
or
curl -H "X-Auth-Token:$TOKEN" $STORAGE_URL/data/test.json
```

6- The Counter micro-controller adds into the object metadata an access counter and the last access timestamt. Verify the correct execution of the micro-controller by running the following command:
```bash
swift stat data test.json
```

### Usage
 