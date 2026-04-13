package com.cs6650.kvstore.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.List;
@Component
@ConfigurationProperties(prefix = "kvstore")
public class KvStoreProperties {

    private String nodeId;
    private String mode;
    private String role;
    private List<String> peers = new ArrayList<>();
    private Quorum quorum = new Quorum();
    private Delays delays = new Delays();

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public List<String> getPeers() {
        return peers;
    }

    public void setPeers(List<String> peers) {
        this.peers = peers;
    }

    public Quorum getQuorum() {
        return quorum;
    }

    public void setQuorum(Quorum quorum) {
        this.quorum = quorum;
    }

    public Delays getDelays() {
        return delays;
    }

    public void setDelays(Delays delays) {
        this.delays = delays;
    }

    public static class Quorum {
        private int write;
        private int read;

        public int getWrite() {
            return write;
        }

        public void setWrite(int write) {
            this.write = write;
        }

        public int getRead() {
            return read;
        }

        public void setRead(int read) {
            this.read = read;
        }
    }

    public static class Delays {
        private long writeDelayMs;
        private long readDelayMs;

        public long getWriteDelayMs() {
            return writeDelayMs;
        }

        public void setWriteDelayMs(long writeDelayMs) {
            this.writeDelayMs = writeDelayMs;
        }

        public long getReadDelayMs() {
            return readDelayMs;
        }

        public void setReadDelayMs(long readDelayMs) {
            this.readDelayMs = readDelayMs;
        }
    }
}