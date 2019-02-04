### Flunx - Flux language on Flink engine

The mapping from a [Flux](www.github.com/influxdata/flux) specification is yet to be completed, this is a naive implementation.  

To run a simple example, download [Flink binaries](https://archive.apache.org/dist/flink/flink-1.6.1/) and:

```
$ ./bin/flux compile @./queries/gen.flux > gen.spec
$ mvn package
$ ./flink-1.6.1/bin/flink run ./target/flunx.jar --spec ./gen.spec
```
