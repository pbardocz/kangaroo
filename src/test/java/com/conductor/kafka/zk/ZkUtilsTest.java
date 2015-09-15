/**
 * Copyright 2014 Conductor, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 * 
 */

package com.conductor.kafka.zk;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.conductor.kafka.Broker;
import com.conductor.kafka.Partition;
import com.conductor.kafka.zk.ZkUtils.StringSerializer;
import com.google.common.collect.Lists;

/**
 * @author cgreen
 */
@RunWith(MockitoJUnitRunner.class)
public class ZkUtilsTest {

    @Mock
    private ZkClient client;

    private ZkUtils zk;

    @Before
    public void setUp() throws Exception {
        zk = spy(new ZkUtils(client, "/"));
    }

    @Test
    public void testConstructor() throws Exception {
        // with default namespace
        assertEquals("", zk.getZkRoot());

        // non-default namespace, tailing "/"
        zk = new ZkUtils(client, "/com/conductor/");
        assertEquals("/com/conductor", zk.getZkRoot());
    }

    @Test
    public void testGetBrokerId() throws Exception {
        // normal case
        final int brokerId = 1;
        when(client.readData("/brokers/ids/" + brokerId, true)).thenReturn("{\"jmx_port\":9999,\"timestamp\":\"1438181977184\",\"host\":\"127.0.0.1\",\"version\":1,\"port\":9092}");
        final Broker broker = zk.getBroker(brokerId);
        assertEquals("127.0.0.1", broker.getHost());
        assertEquals(9092, broker.getPort());
        assertEquals(brokerId, broker.getId());

        // non-existant broker
        when(client.readData("/brokers/ids/" + 2, true)).thenReturn(null);
        final Broker nonExistantBroker = zk.getBroker(2);
        assertNull(nonExistantBroker);
    }

    @Test
    public void testGetBrokers() throws Exception {
        final List<String> brokerIds = Lists.newArrayList("1", "2");
        final Broker b1 = new Broker("localhost", 9092, 1);
        final Broker b2 = new Broker("localhost", 9092, 2);

        when(client.getChildren("/brokers/ids")).thenReturn(brokerIds);
        doReturn(b1).when(zk).getBroker(1);
        doReturn(b2).when(zk).getBroker(2);

        final List<Broker> brokers = zk.getBrokers();
        assertTrue(brokers.contains(b1));
        assertTrue(brokers.contains(b2));
    }

    @Test
    public void testPartitions() throws Exception {
        final Broker broker1 = new Broker("localhost", 9092, 0);
        final Broker broker2 = new Broker("localhost", 9092, 1);
        final List<Broker> brokerList = Lists.newArrayList(broker1, broker2);
        final String brokerTopicPath = "/brokers/topics/the_topic";
        final String brokerPartitionInfo = "{\"version\":1,\"partitions\":{\"0\":[0],\"1\":[1],\"2\":[0],\"3\":[1]}}";

        when(client.readData(brokerTopicPath)).thenReturn(brokerPartitionInfo);

        doReturn(broker1).when(zk).getBroker(0);
        doReturn(broker2).when(zk).getBroker(1);

        final List<Partition> result = zk.getPartitions("the_topic");
        assertEquals(4, result.size());

        for (int i = 0; i < 4; ++i) {
            final Partition part1 = new Partition("the_topic", i, brokerList.get(i%2));
            assertTrue(result.contains(part1));
        }
    }

    @Test
    public void testPartitionExists() throws Exception {
        final Broker broker = new Broker("localhost", 9092, 1);
        when(client.readData("/brokers/topics/the_topic/1", true)).thenReturn("10");

        assertTrue(zk.partitionExists(broker, "the_topic", 0));
        assertTrue(zk.partitionExists(broker, "the_topic", 9));
        assertFalse(zk.partitionExists(broker, "the_topic", 10));

        when(client.readData("/brokers/topics/the_topic/1", true)).thenReturn(null);
        assertFalse(zk.partitionExists(broker, "the_topic", 5));
    }

    @Test
    public void testGetLastCommit() throws Exception {
        final Partition partition = new Partition("topic", 1, null);
        doReturn("/a/path").when(zk).getOffsetsPath("group", partition);
        when(client.readData("/a/path", true)).thenReturn("1234567");

        assertEquals(1234567, zk.getLastCommit("group", partition));

        when(client.readData("/a/path", true)).thenReturn(null);
        assertEquals(-1, zk.getLastCommit("group", partition));
    }

    @Test
    public void testSetLastCommit() throws Exception {
        final Partition partition = new Partition("topic", 1, null);
        doReturn("/a/temp-offset/").when(zk).getTempOffsetsPath("group", partition);
        doReturn("/a/offset/").when(zk).getOffsetsPath("group", partition);
        when(client.exists("/a/temp-offset/")).thenReturn(true);
        when(client.exists("/a/offset/")).thenReturn(true);

        zk.setLastCommit("group", partition, 10l, true);
        verify(client, times(1)).writeData("/a/temp-offset/", 10l);
        verify(client, never()).createPersistent("/a/temp-offset/", true);

        zk.setLastCommit("group", partition, 10l, false);
        verify(client, times(1)).writeData("/a/offset/", 10l);
        verify(client, never()).createPersistent("/a/offset/", true);

        when(client.exists("/a/temp-offset/")).thenReturn(false);
        zk.setLastCommit("group", partition, 10l, true);
        verify(client, times(1)).createPersistent("/a/temp-offset/", true);
    }

    @Test
    public void testCommit() throws Exception {
        final Partition partition1 = new Partition("the_topic", 0, null);
        final Partition partition2 = new Partition("the_topic", 1, null);
        final List<String> brokerIds = Lists.newArrayList("0", "1");
        final String brokerTopicPath = zk.getTempOffsetsSubPath("the_group", "the_topic");

        doNothing().when(zk).setLastCommit(anyString(), any(Partition.class), anyLong(), anyBoolean());
        doReturn(brokerIds).when(zk).getChildrenParentMayNotExist(brokerTopicPath);

        final String tempOffsetPath1 = zk.getTempOffsetsPath("the_group", partition1);
        final String tempOffsetPath2 = zk.getTempOffsetsPath("the_group", partition2);

        when(client.readData(tempOffsetPath1)).thenReturn("1234567");
        when(client.readData(tempOffsetPath2)).thenReturn("12345678");

        zk.commit("the_group", "the_topic");

        // reads the data from the temp offset
        verify(client, times(1)).readData(tempOffsetPath1);
        verify(client, times(1)).readData(tempOffsetPath2);
        verify(client, times(2)).readData(anyString());

        // sets the last commit for each partition
        verify(zk, times(1)).setLastCommit("the_group", partition1, 1234567l, false);
        verify(zk, times(1)).setLastCommit("the_group", partition2, 12345678l, false);
        verify(zk, times(2)).setLastCommit(anyString(), any(Partition.class), anyLong(), anyBoolean());

        // deletes the temp offsets
        verify(client, times(1)).delete(tempOffsetPath1);
        verify(client, times(1)).delete(tempOffsetPath2);
        verify(client, times(2)).delete(anyString());
    }

    @Test
    public void getGetPaths() throws Exception {
        final Broker broker = new Broker("localhost", 9092, 1);
        final Partition partition = new Partition("topic_name", 0, broker);
        // consumer
        assertEquals("/consumers/group_name/offsets/topic_name/0", zk.getOffsetsPath("group_name", partition));
        assertEquals("/consumers/group_name/offsets-temp/topic_name/0",
                zk.getTempOffsetsPath("group_name", partition));
        assertEquals("/consumers/group_name/offsets-temp/topic_name",
                zk.getTempOffsetsSubPath("group_name", "topic_name"));

        // broker-id
        assertEquals("/brokers/ids/1", zk.getBrokerIdPath(1));
        assertEquals("/brokers/ids", zk.getBrokerIdSubPath());

        // broker-topic
        assertEquals("/brokers/topics/topic_name", zk.getTopicBrokerIdSubPath("topic_name"));
        assertEquals("/brokers/topics/topic_name/1", zk.getTopicBrokerIdPath("topic_name", 1));
    }

    @Test
    public void testZkSerializer() throws Exception {
        final StringSerializer serDe = new StringSerializer();
        assertNull(serDe.deserialize(null));
        assertEquals("test", serDe.deserialize("test".getBytes()));
        assertArrayEquals("test".getBytes(), serDe.serialize("test"));
        assertEquals("test", serDe.deserialize(serDe.serialize("test")));
    }

    @Test
    public void testCreatePathWithZkRoot(){
        assertEquals("/brokers/topics/topic_name", zk.createPathWithZkRoot("brokers/topics/topic_name"));
    }
}
