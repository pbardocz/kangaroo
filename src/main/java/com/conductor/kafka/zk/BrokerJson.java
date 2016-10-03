package com.conductor.kafka.zk;

import java.util.ArrayList;

/**
 * Created by pbardocz on 8/4/15.
 */
public class BrokerJson {

    String jmx_port;
    String timestamp;
    ArrayList<String> endpoints;
    String host;
    Integer port;
    String version;

    public String getJmx_port() {
        return jmx_port;
    }

    public void setJmx_port(String jmx_port) {
        this.jmx_port = jmx_port;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public ArrayList<String> getEndpoints() {
        return endpoints;
    }

    public void setEndpoints(ArrayList<String> endpoints) {
        this.endpoints = endpoints;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Integer getPort() {
        return port;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
