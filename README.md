
With target URI:
 
<pre>
etcd://hostname:port/my_service
</pre>
 
And these KVs:
 
```bash
# the value won't make any different
etcdctl put /my_service/group1/127.0.0.1:8080 1
etcdctl put /my_service/group1/127.0.0.1:8081 1
etcdctl put /my_service/group2/127.0.0.1:8082 1
```
 
Will resolve to:
 
```java
List[
EquivalentAddressGroup { addresses: [127.0.0.1:8080, 127.0.0.1:8081]},
EquivalentAddressGroup { addresses: [127.0.0.1:8082]}
]
```

Build from source:
```bash
./gradlew build
# then grab the jar file on directory ./build/libs/
```