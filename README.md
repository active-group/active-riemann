# active-riemann

A library with common Riemann functionality that is useful in many Riemann
setups.

[![Clojars Project](https://img.shields.io/clojars/v/de.active-group/active-riemann.svg)](https://clojars.org/de.active-group/active-riemann)

## Usage

Use

```
lein jar
```

to generate a JAR file under `target/`.  Move this JAR file into your Riemann
configuration directory.

Then you can use

```
;; add implementation to classpath
(require '[cemerick.pomegranate :as pomegranate])
(pomegranate/add-classpath "active-riemann-0.1.0-SNAPSHOT.jar")
```

to include the library in your Riemann configuration file.

Now you can use the provided functionality:

### Elasticsearch connector

```
(require '[active-riemann.elasticsearch :as elasticsearch])
(elasticsearch/make-elasticsearch-stream elasticsearch-url index-base-name)
```

The Elasticsearch connector provides retry via single submit when batch
submitting fails and prints debugging information if single submit also fails.

### InfluxDB connector

```
(require '[active-riemann.influxdb :as influxdb])
(influxdb/make-influxdb-stream influxdb-host db-nbame tag-fields)
```

## License

Copyright © 2020-2026 Active Group GmbH

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
