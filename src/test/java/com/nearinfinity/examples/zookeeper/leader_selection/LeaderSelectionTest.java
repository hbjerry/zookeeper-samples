package com.nearinfinity.examples.zookeeper.leader_selection;

import com.nearinfinity.examples.zookeeper.lock.BlockingWriteLock;
import com.nearinfinity.examples.zookeeper.util.ConnectionHelper;
import com.nearinfinity.examples.zookeeper.util.RandomAmountOfWork;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: leizh
 * Date: 1/5/14
 * Time: 7:43 PM
 * To change this template use File | Settings | File Templates.
 */
public class LeaderSelectionTest {

    private String hosts = "strokingjoking.corp.gq1.yahoo.com:2181";
    private String leaderPath = "/leader";

    @Test
    public void testRun() throws Exception {
        ConnectionHelper connectionHelper = new ConnectionHelper();
        ZooKeeper zooKeeper = connectionHelper.connect(hosts);
        LeaderSelection leaderSelection = new LeaderSelection(zooKeeper, leaderPath, ZooDefs.Ids.OPEN_ACL_UNSAFE,

                new LeaderListener() {
            @Override
            public void leaderAcquired() {
                System.out.println("I am Leader, haha!");
            }
        });
        leaderSelection.run();
        Thread.sleep(Long.MAX_VALUE);

    }

    private static void doSomeWork(String name) {
        int seconds = new RandomAmountOfWork().timeItWillTake();
        long workTimeMillis = (seconds + 10) * 1000;
        System.out.printf("%s is doing some work for %d seconds\n", name, seconds);
        try {
            Thread.sleep(workTimeMillis);
        }
        catch (InterruptedException ex) {
            System.out.printf("Oops. Interrupted.\n");
            Thread.currentThread().interrupt();
        }
    }
}
