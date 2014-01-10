package com.nearinfinity.examples.zookeeper.leader_selection;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.recipes.lock.ZNodeName;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA.
 * User: leizh
 * Date: 1/5/14
 * Time: 7:12 PM
 * To change this template use File | Settings | File Templates.
 */
public class LeaderSelection implements Watcher, AsyncCallback.StatCallback {

    private final String dir;
    private String id;
    private String ownerId;
    private String lastChildId;
    private byte[] data = {0x12, 0x34};
    private LeaderListener callback;
    private ZooKeeper zooKeeper;
    private List<ACL> acl;
    private String path;

    private final String NODE_PREFIX = "n-test-";

    public LeaderSelection(ZooKeeper zookeeper, String dir, List<ACL> acl, LeaderListener callback) {
        this.zooKeeper = zookeeper;
        this.dir = dir;
        this.acl = acl;
        this.callback = callback;
    }
    @Override
    public void processResult(int rc, String path, Object ctx, Stat stat){
        System.out.println("path:" + path);
        if (stat != null){
            System.out.println("the stat:" + stat);
        }
        else{
            System.out.println("the last child not exists");
            callback.leaderAcquired();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        // lets either become the leader or watch the new/updated node
        System.out.println("Watcher fired on path: " + event.getPath() + " state: " +
                event.getState() + " type " + event.getType());
        if (event.getType() == Event.EventType.NodeDeleted){
            System.out.println("the last child is removed");
            callback.leaderAcquired();
        }
    }

    void run() throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(dir, false);
        if (stat == null) {
            zooKeeper.create(dir, data, acl, CreateMode.PERSISTENT);
        }

        path = zooKeeper.create(dir + "/" + NODE_PREFIX,data,acl,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Create node under:" + dir + " path:" + path);
        ZNodeName idName = new ZNodeName(path);

        List<String> names = zooKeeper.getChildren(dir, false);

        // lets sort them explicitly (though they do seem to come back in order ususally :)
        SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
        for (String name : names) {
            sortedNames.add(new ZNodeName(dir + "/" + name));
        }
        //SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(new ZNodeName("n-test-0000000013"));
        SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(idName);
        if (!lessThanMe.isEmpty()) {
            lastChildId = lessThanMe.last().getName();
            System.out.println("get the last child:" + lastChildId);
            zooKeeper.exists(lastChildId, this, this, null);

        } else {
            System.out.println("No children in: " + dir + " i am the leader");
            callback.leaderAcquired();
        }
    }
}
