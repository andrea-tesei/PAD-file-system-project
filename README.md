# PAD file system project

For the final project of Distributed Enabling Platform exam, i choose to develop the <b>PADfs</b> project which is a distributed persistent data storage which accept and save all type of data as key-value pairs (filename, content). For simplicity i choose a flat structure of the file system: so all data inside must have a different file name.

# How to setting up a node

In order to setting up a single node, you can execute the following command from the root directory of the <b>PAD-file-system-project</b> repository:
```
mvn clean package
```
this will compile all dependencies needed by PADfs project. Then you can execute the single node by running the following command:
```
java -jar PADfs-1.0.jar gossip.conf
```
where gossip.conf is the JSON configuration file of gossiping protocol and it has the given structure:

1. <b>id</b>: identifier of the running node (you can use UUID or whatever you want);
2. <b>port</b>: udp port for the gossiping protocol;
3. <b>gossip_interval</b>: the time between two consecutive Gossip messages;
4. <b>cleanup_interval</b>: the timer length for failure detection;
5. <b>hostname</b>: the hostname of the running node (previously set in /etc/hosts);
6. <b>members</b>: array of other nodes with IP address, port and id.

Make sure that the /etc/hosts file of the machines in which you want to run a PADfs instance are configured correctly: the hostname of the node given to the <b>gossip.conf</b> must be associated to the ip of the machine and also ip addresses of other nodes with their hostname have to be set in /etc/hosts.

# Administrative command-line

Each node of the client is equipped with four basic operations for administrative control. Once you have executed a single node, you can use the following commands to know the status of the cluster:

1. <b>CLUSTER_STATUS</b>: prints all nodes with UP/DOWN flag and ip address;
2. <b>NODE2FILEMAPPING</b>: for each node, print all files store on it;
3. <b>HELP</b>: print a list of available commands and explanation;
3. <b>QUIT</b>: to power off the node.

# PAD file system client

I've developed also a thin client which offers the basic operations to the user. An user can send a request to each active node of the cluster which will perform the operation even if the requested file is not local to the contacted node. The client uses the REST APIs actually attached to each node of the cluster.
The operation exposed to the clients are five:

1. <b>GET</b>: for download a file from the file system; (Syntax: GET:filename)
2. <b>PUT</b>: for put in the storage new file, or override/update an existing one; (Syntax: PUT:filename)
3. <b>LIST</b>: for retrieving the list of files currently on the file system; (Syntax: LIST)
3. <b>DELETE</b>: for deletion of a file currently stored in the file system; (Syntax: DELETE:filename)
4. <b>QUIT</b>: close the client.

To execute the <b>PADfs-cli</b>, you have first to compile it by running:
```
mvn clean package
```
in the <b>PADfs-cli</b> folder, and once terminated you can execute the following command:
```
java –jar PADfs-cli.jar known_hosts.conf
```
where <b>known_hosts.conf</b> is the list of known active nodes (Syntax: one IP address for each line).

# Test phases

All tests were performed by using five [vagrant VMs](https://www.vagrantup.com/) and the final project has been tested also in a real cluster of seven nodes available at [ISTI of CNR](http://www.isti.cnr.it/).  

# Further information

For further information abount the architecture, design choices, code structure and other stuff, you can read my [final report](https://github.com/andrea-tesei/PAD-file-system-project/blob/master/Report_PADfs_Tesei.pdf).
