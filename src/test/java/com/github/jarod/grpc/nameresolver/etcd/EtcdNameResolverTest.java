package com.github.jarod.grpc.nameresolver.etcd;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.KV;
import com.coreos.jetcd.data.ByteSequence;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.NameResolver.Listener;
import io.grpc.Status;

public class EtcdNameResolverTest {
	private EtcdNameResolverProvider provider = new EtcdNameResolverProvider();

	@Test
	@Tag("slow")
	public void newNameResolver() throws Exception {
		String etcdAddress = "127.0.0.1:2379";
		String serviceKey = "/grpc/test_service";
		Client etcd = Client.builder().endpoints("http://" + etcdAddress).build();
		KV kv = etcd.getKVClient();
		HostAndPort[] hap = new HostAndPort[] { HostAndPort.fromString("127.0.0.1:8080"),
				HostAndPort.fromString("127.0.0.1:8081"), HostAndPort.fromString("127.0.0.1:8082"), };
		EquivalentAddressGroup g1 = new EquivalentAddressGroup(
				Lists.newArrayList(new InetSocketAddress(hap[0].getHost(), hap[0].getPort()),
						new InetSocketAddress(hap[1].getHost(), hap[1].getPort())));
		kv.put(ByteSequence.fromString(serviceKey + "/group1/" + hap[0].toString()), ByteSequence.fromString("1"));
		kv.put(ByteSequence.fromString(serviceKey + "/group1/" + hap[1].toString()), ByteSequence.fromString("1"));

		EquivalentAddressGroup g2 = new EquivalentAddressGroup(
				Lists.newArrayList(new InetSocketAddress(hap[2].getHost(), hap[2].getPort())));
		kv.put(ByteSequence.fromString(serviceKey + "/group2/" + hap[2].toString()), ByteSequence.fromString("1"));

		NameResolver resolver = provider
				.newNameResolver(URI.create(String.format("etcd://%s%s", etcdAddress, serviceKey)), Attributes.EMPTY);
		CountDownLatch latch = new CountDownLatch(1);
		resolver.start(new Listener() {

			@Override
			public void onAddresses(List<EquivalentAddressGroup> servers, Attributes attributes) {
				latch.countDown();
				for (EquivalentAddressGroup g : servers) {
					String group = g.getAttributes().get(EtcdNameResolver.KEY_GROUP);
					if (group.equals("group1")) {
						assertEquals(g1.getAddresses(), g.getAddresses());
					} else if (group.equals("group2")) {
						assertEquals(g2.getAddresses(), g.getAddresses());
					}
				}

			}

			@Override
			public void onError(Status error) {
				latch.countDown();
				fail(error.getDescription(), error.getCause());
			}
		});
		assertTrue(latch.await(5L, TimeUnit.SECONDS));
	}
}