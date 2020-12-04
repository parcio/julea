# Container

## Docker

Root privileges are required on the system.

### Docker Image

In the root directory of the repository is a Dockerfile that loads the standard dependencies 
and configures JULEA using Meson and Ninja. By default JULEA files are copied to **/julea**. 

The Dockerfile can be modified to create a new image. 

```
docker build -t julea-standard:1.0 .
```

The image can be published on Dockerhub. For this, the following commands must be executed. 
Further information can be found here: https://docs.docker.com/docker-hub/repos/

```
docker tag julea-standard:1.0 <DOCKER ID>/julea-standard:1.0
docker push <DOCKER ID>/julea-standard:1.0
```

### Use Docker Image

In the directory **./container/docker** a JULEA-Server and a JULEA-Client and their interaction are shown as an example.
For background information on how the Docker network works read the following docs:  https://docs.docker.com/config/containers/container-networking/

#### Network Communication between containers
If docker-compose is not an option a network must be created for the communication between the different containers.
All containers that are supposed to interact with each other have set this network as a parameter.

```
docker network create myNet
```

#### Server

The Dockerfile of the server uses the JULEA image from Dockerhub. In the same folder a script is created to configure the server. 
Read the next chapter for more detailed background information. 
The script loads the environment variables, sets the configuration of JULEA by *julea-config* and then starts a server by *julea-server*.
Also, it adds the execution of the environment script to **.bashrc** so that the variables are load when entering the container.
This need to be done otherwise the JULEA commands are not available within the container.

In the dockerfile this script is copied into the image. The default port for ther JULEA server is exposed.
At the end the *ENTRYPOINT* is defined which calls the script when *docker run* is executed. 
The default values for the script are provided by the *CMD* command (hostname: **juleaServer**, port number: **9876**). 

```
cd ./container/docker/server
docker build -t julea-server-image .
docker run -it -d --name julea-server julea-server-image
docker exec -ti julea-server /bin/bash
```

If the default values should be overwritten add the requested values at the end of the statement

```
docker run -it -d --name julea-server julea-server-image SERVERNAME PORT
```

For network communication, the hostname and port number given in the script must be set here. In addition the created network must be specified.
```
docker run -it -d --network-alias juleaServer -p 9876:9876/tcp --network myNet --name julea-server julea-server-image
```

#### Client
The client uses basically the same procedure. Here the hostname and port number of the JULEA server are specified in the *CMD* command of the Dockerfile.
If the default values are overwritten, this must happen in the client as well. 
In the script a *tail* command is executed at the end, which ensures that the container is not shut down immediately after starting. 
This would normally happen because no permanent process is running.

```
cd ./container/docker/client
docker build -t julea-client-image .
docker run -it -d --name julea-client julea-client-image
docker exec -ti julea-client /bin/bash
```

If the default values should be overwritten:
```
docker run -it -d --name julea-client julea-client-image SERVERNAME PORT
```

To be able to communicate with the server, the network must also be specified.
```
docker run -it -d --network myNet --name julea-client julea-client-image
```

### Why to a script in ENTRYPOINT
The server's Dockerfile only contains the *ENTRYPOINT* that refers to a script. 
Within the script the configuration for JULEA is done by *julea-config* and the server is started. 
Now you might wonder why this configuration is not set directly in the Dockerfile. 
For that it is important to know that every *RUN* command in the Dockerfile creates its own layer. 
The environment variables, which are set by the environment script, are not persistent. 
That means that another layer can't access them, which leads to that, 
that with two *RUN* statements the command *julea-config* is no longer available and an error occurs. 
An *ENTRYPOINT* that could start e.g. a JULEA server has a similar problem. 
Again, the environment variables cannot be accessed, they would have to be set again to find the *julea-config* or *julea-server* command. 
To do the whole configuration inside the Dockerfile finally becomes much too complicated and unclear. 
Therefore it is recommended to outsource the configuration to an external script.

### Volume
To be able to develop more dynamically, there is the possibility to mount local directories into the container. 
So you could develop or use JULEA on the local host and then compile and execute the result in the container.
As an example the client is extended here. In the client directory is a folder **build** which contains the example files from the JULEA project. 
These files are mounted into the container and will be available under **/build**.

```
docker run -it -d --network myNet -v /absolute/path/to/dir/build:/build --name julea-client julea-client-image
```

### Docker-Compose
To simplify the administration of containers there is a docker-compose example in the root directory of JULEA. 
In this example a server and a client is created. The name of the service of juleaServer is the server name, 
which must be set for the client, otherwise the server cannot be reached from the client. 
For the server the ports are defined, which are visible to the hostsystem and other containers.
In addition a name for the image and one for the container for both services is defined, so that no random ones are generated and remain unique.
For the client, a directory is mounted from the host into the container.

Creating the network is no longer necessary, because docker-compose does this. For this example a network is created, 
which is called *julea_default*. This can be examined with *docker network inspect julea_default*.

To start both containers, just type:
```
docker-compose up -d
```

## Singularity
No root privileges are required on the system.

Logs can be found here: ~/.singularity/instances/logs/HOSTNAME/USERNAME/

## Singularity Image
All commands (exec, run and shell) use the image as read-only. To make changes to the image, the --writeable flag can be set. This requires root privileges. 
To use Singularity with JULEA, e.g. to compile files in the image, a directory with the files can be mounted to the container. 
With *julea-config* this directory will be set as output directory.

In addition, by default some directories from the host system are mounted into the image. Read about this here: 
https://sylabs.io/guides/3.0/user-guide/bind_paths_and_mounts.html#system-defined-bind-paths

The singularity file uses the Docker images from Dockerhub.

### Server
The Singularity server image copies a script into the image, which is executed when the instance is started.
This script sets the configuration for JULEA. The path for the backends is set here to the mounted directory.
Then the server is started with the hostname *juleaserver* and the port *9876*.
```
cd ./container/singularity/server
singularity build --fakeroot julea-server.sif Singularity
singularity instance start --bind singularity-mnt/:/singularity-mnt julea-server.sif julea-server-instance
```

For the client, or other system, the IP address of the server is needed. This can be found out as follows.
The IP should be in subnet 10.X.X.X.
```
singularity exec instance://julea-client-instance hostname -I
```

### Client
The Singularity client image proceeds the same way as the server image. 
Again, a script is created that uses the hostname and port of the server.
In order to resolve the hostname of the server, the following step must be executed.
Alternatively you can also work with the IP address of the server, then this step is not necessary.

Create an *etc.hosts* file in the directory of the client. 
Add the IP address of the server and mount this file into the instance.
```
cd ./container/singularity/client
singularity build --fakeroot julea-client.sif Singularity
singularity instance start --bind etc.hosts/:/etc/hosts --bind singularity-mnt/:/singularity-mnt julea-client.sif julea-client-instance
singularity shell instance://julea-client-instance
```

If you want to work with JULEA in the container, make sure that the environment variables are loaded.
```
. /julea/scripts/environment.sh
```

## Podman
The Docker images and Dockerfiles are also supported by Podman. 
Here you can use the same commands as for Docker. 
Just replace *docker* with *podman* at the beginning of the command.

### Podman-Compose
To use the *docker-compose.yml* with Podman, the following project can be used: https://github.com/containers/podman-compose
