package com.github.jarod.grpc.nameresolver.etcd;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;

import org.junit.jupiter.api.Test;

import io.grpc.Attributes;

public class EtcdNameResolverProviderTest {
    private EtcdNameResolverProvider provider = new EtcdNameResolverProvider();

    @Test
    public void isAvailable() {
        assertTrue(provider.isAvailable());
    }

    @Test
    public void newNameResolver() {
        assertSame(EtcdNameResolver.class,
                provider.newNameResolver(URI.create("etcd://localhost:2379/service"), Attributes.EMPTY).getClass());
        assertSame(EtcdNameResolver.class,
                provider.newNameResolver(URI.create("etcd://localhost/without_port"), Attributes.EMPTY).getClass());
        assertNull(provider.newNameResolver(URI.create("notetcd://localhost:2379/service"), Attributes.EMPTY));
    }
}