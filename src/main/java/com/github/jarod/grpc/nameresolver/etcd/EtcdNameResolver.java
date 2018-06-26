package com.github.jarod.grpc.nameresolver.etcd;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;

import com.coreos.jetcd.Client;
import com.coreos.jetcd.Watch.Watcher;
import com.coreos.jetcd.data.ByteSequence;
import com.coreos.jetcd.data.KeyValue;
import com.coreos.jetcd.options.GetOption;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Attributes;
import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import io.grpc.Attributes.Key;
import io.grpc.internal.GrpcUtil;

public class EtcdNameResolver extends NameResolver {
    static class Address {
        String group;
        SocketAddress address;

        Address(String group, SocketAddress addr) {
            this.group = group;
            this.address = addr;
        }
    }

    static Key<String> KEY_GROUP = Key.create("GROUP");

    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Client etcdClient;
    private final ByteSequence keyPrefix;
    private Listener listener;
    @GuardedBy("this")
    private boolean shutdown;
    private final String authority;
    private Watcher watcher;
    @GuardedBy("this")
    private final HashMultimap<String, SocketAddress> addresses = HashMultimap.create();
    private Thread watchThread;
    private final ScheduledExecutorService executor;
    @GuardedBy("this")
    private ScheduledFuture<?> watchResultFuture;

    EtcdNameResolver(Client client, String authority, String keyPrefix) {
        etcdClient = client;
        this.authority = authority;
        this.keyPrefix = ByteSequence.fromString(keyPrefix);
        watchThread = new Thread(() -> watch());
        executor = GrpcUtil.TIMER_SERVICE.create();
    }

    @Override
    public String getServiceAuthority() {
        return authority;
    }

    @Override
    public void start(Listener listener) {
        Preconditions.checkState(this.listener == null, "already started");
        this.listener = Preconditions.checkNotNull(listener, "listener");

        resolve();

        watcher = etcdClient.getWatchClient().watch(keyPrefix, WatchOption.newBuilder().withPrefix(keyPrefix).build());
        watchThread.start();
    }

    // @Override
    // public void refresh() {
    // resolve();
    // }

    @Override
    public synchronized void shutdown() {
        if (shutdown) {
            return;
        }
        shutdown = true;
        etcdClient.close();
        GrpcUtil.TIMER_SERVICE.close(executor);
    }

    private Optional<Address> parseKey(String key) {
        List<String> keyParts = Splitter.on("/").splitToList(key);
        if (keyParts.size() < 3) {
            logger.warn("key (%s) ignored, key must follow format /service/group/hostname:port", key);
            return Optional.empty();
        }
        URI uri = URI.create("grpc://" + keyParts.get(keyParts.size() - 1));
        SocketAddress addr = new InetSocketAddress(uri.getHost(), uri.getPort());
        return Optional.of(new Address(keyParts.get(keyParts.size() - 2), addr));
    }

    private boolean addAddress(KeyValue kv) {
        String key = kv.getKey().toStringUtf8();
        Optional<Address> addr = parseKey(key);
        addr.ifPresent(a -> {
            synchronized (this) {
                addresses.put(a.group, a.address);
            }
        });
        return addr.isPresent();
    }

    private void watch() {
        boolean stop = false;
        do {
            try {
                for (WatchEvent e : watcher.listen().getEvents()) {
                    switch (e.getEventType()) {
                    case PUT:
                        addAddress(e.getKeyValue());
                        break;
                    case DELETE:
                        parseKey(e.getKeyValue().getKey().toStringUtf8()).ifPresent(a -> {
                            synchronized (EtcdNameResolver.this) {
                                addresses.remove(a.group, a.address);
                            }
                        });
                        break;
                    default:
                        // ignore
                    }
                }
                // aggregate all event within 2 secs
                synchronized (this) {
                    if (watchResultFuture != null) {
                        continue;
                    }
                    watchResultFuture = executor.schedule(() -> {
                        listener.onAddresses(buildAddressGroups(), Attributes.EMPTY);
                        synchronized (EtcdNameResolver.this) {
                            watchResultFuture = null;
                        }
                    }, 2L, TimeUnit.SECONDS);
                }
            } catch (Exception ex) {
                listener.onError(Status.fromThrowable(ex));
            }
            synchronized (this) {
                stop = shutdown;
            }
        } while (!stop);
    }

    private List<EquivalentAddressGroup> buildAddressGroups() {
        List<EquivalentAddressGroup> ret = Lists.newArrayListWithExpectedSize(4);
        synchronized (this) {
            for (String group : addresses.keySet()) {
                EquivalentAddressGroup ag = new EquivalentAddressGroup(Lists.newArrayList(addresses.get(group)),
                        Attributes.newBuilder().set(KEY_GROUP, group).build());
                ret.add(ag);
            }
        }
        return ret;
    }

    private void resolve() {
        etcdClient.getKVClient().get(keyPrefix, GetOption.newBuilder().withPrefix(keyPrefix).build())
                .thenApply((res) -> {
                    synchronized (EtcdNameResolver.this) {
                        addresses.clear();
                    }
                    res.getKvs().forEach(kv -> addAddress(kv));
                    return buildAddressGroups();
                }).whenComplete((v, ex) -> {
                    if (ex != null) {
                        listener.onError(Status.fromThrowable(ex));
                    } else {
                        listener.onAddresses(v, Attributes.EMPTY);
                    }
                });
    }
}