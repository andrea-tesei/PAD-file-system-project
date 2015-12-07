# Gossip

Gossip protocol is a method for a group of nodes to discover and check the liveliness of a cluster. More information can be found at http://en.wikipedia.org/wiki/Gossip_protocol.

The original implementation was forked from https://github.com/edwardcapriolo/gossip (that was forked from https://code.google.com/p/java-gossip/). Several bug fixes and changes have already been added.

Command-Line Usage
-----

From command line, you can start a gossip service with a single seed node by using:

```bash
cd gossiping
mvn clean compile
java -cp target/gossip-0.0.3-jar-with-dependencies.jar com.google.code.gossip.GossipRunner <port> <seed ip>
```

Alternatively, you can provide a configuration file at startup:

```bash
java -cp gossip.jar com.google.code.gossip.GossipRunner <config file>
```

If no arguments are provided, the program will look up for the file `gossip.conf` in current directory.

The structure of a configuration file is as in the following example (note the JSON syntax):

```json
[{
	"port":2000,
 	"log_level":"DEBUG",
 	"gossip_interval":1000,
 	"cleanup_interval":10000,
 	"members": 
 	[
  		{"host":"node1.test","port":"2000"},
  		{"host":"node2.test","port":"2000"},
  		{"host":"node3.test","port":"2000"}
 	]
}]
```
Usage
-----

To gossip you need one or more seed nodes. Seed is just a list of places to initially connect to.

```java
  GossipSettings settings = new GossipSettings();
  int seedNodes = 3;
  List<GossipMember> startupMembers = new ArrayList<>();
  for (int i = 1; i < seedNodes+1; ++i) {
    startupMembers.add(new RemoteGossipMember("127.0.0." + i, 2000, i + ""));
  }
```

Here we start five gossip processes and check that they discover each other. (Normally these are on different hosts but here we give each process a distinct local ip.

```java
  List<GossipService> clients = new ArrayList<>();
  int clusterMembers = 5;
  for (int i = 1; i < clusterMembers+1; ++i) {
    GossipService gossipService = new GossipService("127.0.0." + i, 2000, i + "",
      LogLevel.DEBUG, startupMembers, settings, null);
    clients.add(gossipService);
    gossipService.start();
  }
```

Later we can check that the nodes discover each other

```java
  Thread.sleep(10000);
  for (int i = 0; i < clusterMembers; ++i) {
    Assert.assertEquals(4, clients.get(i).get_gossipManager().getMemberList().size());
  }
```

Event Listener
------

The status can be polled using the getters that return immutable lists.

```java
   List<LocalGossipMember> getMemberList()
   public List<LocalGossipMember> getDeadList()
```

Users can also attach an event listener:

```java
  GossipService gossipService = new GossipService("127.0.0." + i, 2000, i + "", LogLevel.DEBUG,
          startupMembers, settings,
          new GossipListener(){
    @Override
    public void gossipEvent(GossipMember member, GossipState state) {
      System.out.println(member+" "+ state);
    }
  });
```


Maven
------

If you want to use this software with another Java project, you don't need to copy the code or the Jar file. You can get this software from maven central. Just include the following dependency in the project POM file.

```xml
  <dependency>
       <groupId>it.cnr.isti.hpclab</groupId>
      <artifactId>gossip</artifactId>
      <version>0.0.3</version>
  </dependency>
```
