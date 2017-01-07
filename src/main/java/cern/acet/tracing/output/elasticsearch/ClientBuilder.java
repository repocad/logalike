/**
 * Logalike - A stream based message processor
 * Copyright (c) 2015 European Organisation for Nuclear Research (CERN), All Rights Reserved.
 * This software is distributed under the terms of the GNU General Public Licence version 3 (GPL Version 3),
 * copied verbatim in the file “COPYLEFT”.
 * In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue
 * of its status as an Intergovernmental Organization or submit itself to any jurisdiction.
 * <p>
 * Authors: Gergő Horányi <ghoranyi> and Jens Egholm Pedersen <jegp>
 */

package cern.acet.tracing.output.elasticsearch;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;

/**
 * A client builder for Elasticsearch that can build clients with different settings and spawn nodes that is a
 * connection to an elasticsearch cluster. The spawned nodes are seen purely as a client node that does not contain any
 * documents (contrary to a data node).
 *
 * @author jepeders
 */
public class ClientBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientBuilder.class);

    private static final String ES_HOME = System.getProperty("user.dir");
    private final Builder settingsBuilder;
    private List<InetSocketAddress> hosts = Collections.emptyList();

    /**
     * Creates a {@link ClientBuilder} with default settings for a client node.
     */
    public ClientBuilder() {
        this(getDefaultSettings());
    }

    /**
     * Created a {@link ClientBuilder} that uses the given settings.
     *
     * @param settings The {@link Settings} to use when building a {@link Node}.
     */
    public ClientBuilder(Settings settings) {
        this.settingsBuilder = Settings.builder().put(settings);
    }

    /**
     * Creates a {@link ClientBuilder} with the given settings applied when
     * constructing a {@link Node}.
     *
     * @param settingsBuilder The settings to apply at class construction.
     */
    ClientBuilder(Builder settingsBuilder) {
        this.settingsBuilder = settingsBuilder;
    }

    /**
     * Sets the name of the cluster to use when discovering the cluster with multicast.
     *
     * @param clusterName The name of the cluster to discover.
     * @return The same {@link ClientBuilder} with the 'cluster.name' property set.
     */
    public ClientBuilder setClusterName(String clusterName) {
        settingsBuilder.put("cluster.name", clusterName);
        return this;
    }

    /**
     * Sets the host of the cluster to use when discovering the cluster with unicast.
     *
     * @param hosts The hosts to search for when connecting to the cluster.
     * @return The same {@link ClientBuilder} with the 'discovery.zen.ping.unicast.hosts' property set. If the hosts
     * list is empty, we return the same builder.
     */
    public ClientBuilder setHosts(List<InetSocketAddress> hosts) {
        if (hosts.isEmpty()) {
            return this;
        } else {
            this.hosts = hosts;
            return this;
        }
    }

    /**
     * Sets the name of the node as it appears in the Elasticsearch cluster.
     *
     * @param name The name of the node.
     * @return The same {@link ClientBuilder} with the 'node.name' property set.
     */
    public ClientBuilder setNodeName(String name) {
        settingsBuilder.put("node.name", name);
        return this;
    }

    /**
     * Creates a client connection from the settings given in the constructor.
     *
     * @return A {@link Client} if a connection was successfully made.
     * @throws NodeValidationException If the connection to the cluster failed.
     */
    public Client build() throws NodeValidationException, UnknownHostException {
        if (hosts.size() == 0) {
            throw new IllegalArgumentException("No hosts defined; Cannot create a client with no hosts to connect to.");
        }
        PreBuiltTransportClient client = new PreBuiltTransportClient(settingsBuilder.build());
        hosts.forEach(host -> client.addTransportAddress(new InetSocketTransportAddress(host)));
        return client;
    }

    /**
     * Creates a number of default settings that prepares a client connection to an Elasticsearch cluster.
     *
     * @return A {@link Builder} object to be used for the cluster connection.
     */
    static Builder getDefaultSettings() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "localhost";
        }
        LOGGER.info("Set host to {}", hostname);

        //@formatter:off
        return Settings.builder()
                .put("path.home", ES_HOME)
                .put("node.master", false) /* This node should not be master */
                .put("node.data", false) /* Disable data so this node holds no data */
                .put("node.ingest", false) /* This node should not use ingest processing */
                .put("transport.host", hostname)
                .put("http.enabled", false) /* Disable http requests on this node */
                .put("client.transport.sniff", true); /* Sniff the rest of the cluster for redundancy */
        //@formatter:on
    }
}