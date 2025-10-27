## Introduction
A repo to reproduce an issue faced when using nats to write into kv and read constantly. It causes inconsistent write issue in the nats-server.

```shell
curl -X POST "http://localhost:8080/api/kv/write?key=mykey" \
    -H "Content-Type: application/json" \
    -d '{"field1":"value1","field2":"value2"}'
```