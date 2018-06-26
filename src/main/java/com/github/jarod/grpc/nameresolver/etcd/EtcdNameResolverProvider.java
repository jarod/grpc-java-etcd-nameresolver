package com.github.jarod.grpc.nameresolver.etcd;

import java.net.URI;

import com.coreos.jetcd.Client;
import com.google.common.base.Preconditions;

import io.grpc.Attributes;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;

public class EtcdNameResolverProvider extends NameResolverProvider {
    private static final String SCHEME = "etcd";

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 10;
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, Attributes params) {
        if (!SCHEME.equals(targetUri.getScheme())) {
            return null;
        }
        String targetPath = Preconditions.checkNotNull(targetUri.getPath(), "targetPath");
        Preconditions.checkArgument(targetPath.startsWith("/"),
                "the path component (%s) of the target (%s) must start with '/'", targetPath, targetUri);
        int port = targetUri.getPort() == -1 ? 2379 : targetUri.getPort();
        String authority = String.format("%s:%d", targetUri.getHost(), port);
        Client etcdClient = Client.builder().endpoints("http://" + authority).build();
        return new EtcdNameResolver(etcdClient, authority, targetPath);
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

}