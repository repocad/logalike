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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.settings.Settings.Builder;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ClientBuilderTest {

    private static final String CLUSTER_NAME_KEY = "cluster.name";
    private static final String CLUSTER_HOSTS_KEY = "discovery.zen.ping.unicast.hosts";
    private static final List<InetSocketAddress> CLUSTER_HOSTS =
            ImmutableList.of(new InetSocketAddress("host1", 9300), new InetSocketAddress("host2", 9300));

    private Builder mockSettingsBuilder;
    private ClientBuilder builder;

    @Before
    public void setup() {
        mockSettingsBuilder = mock(Builder.class);
        builder = new ClientBuilder(mockSettingsBuilder);
    }

    @Test
    public void canSetClusterName() {
        String name = "test";
        builder.setClusterName(name);
        verify(mockSettingsBuilder).put(CLUSTER_NAME_KEY, name);
    }

    @Test
    public void canSetClusterHostsInSettings() {
        final String expectedHosts = "host1,host2";
        builder.setHosts(CLUSTER_HOSTS);
        verify(mockSettingsBuilder).put(CLUSTER_HOSTS_KEY, expectedHosts);
    }

}
