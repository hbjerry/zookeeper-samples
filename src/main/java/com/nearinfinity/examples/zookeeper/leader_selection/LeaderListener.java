package com.nearinfinity.examples.zookeeper.leader_selection;

/**
 * Created with IntelliJ IDEA.
 * User: leizh
 * Date: 1/5/14
 * Time: 7:11 PM
 * To change this template use File | Settings | File Templates.
 */
public interface LeaderListener {

    /**
     * call back called when the leader
     * is acquired
     */
    public void leaderAcquired();

}
