# metabase-impala-driver

## Usage

Copy [`impala.metabase-driver.jar`](https://github.com/XUJiahua/metabase-impala-driver/releases/tag/v1.0.0-SNAPSHOT-2.6.17) to metabase `plugins` folder.

## Initial tests passed

(works fine for me)

Metabase Release | Driver Version | Impala Version
---------------- | -------------- | --------------
0.35.4           | 1.0.0-SNAPSHOT-2.6.17 | 2.5.0
0.35.4           | 1.0.0-SNAPSHOT-2.6.17 | 3.?

## Building from Source

Download impala JDBC driver from [Cloudera](https://www.cloudera.com/downloads/connectors/impala/jdbc/2-6-17.html)
to local `lib` folder.

```
tree lib

lib
└── ImpalaJDBC42.jar
```

Build:

```
make install-local-jar
make build
```

get driver from `target/uberjar/impala.metabase-driver.jar`
