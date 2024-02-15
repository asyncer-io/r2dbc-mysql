# Reactive Relational Database Connectivity MySQL Implementation
![Maven Central](https://img.shields.io/maven-central/v/io.asyncer/r2dbc-mysql?color=blue)
![LICENSE](https://img.shields.io/github/license/asyncer-io/r2dbc-mysql)

This project contains the [MySQL][m] implementation of the [R2DBC SPI](https://github.com/r2dbc/r2dbc-spi).
This implementation is not intended to be used directly, but rather to be
used as the backing implementation for a humane client library to
delegate to. See [R2DBC Homepage](https://r2dbc.io).

See [R2DBC MySQL wiki](https://github.com/asyncer-io/r2dbc-mysql/wiki) for more information.

## Spring-framework and R2DBC-SPI Compatibility
Refer to the table below to determine the appropriate version of r2dbc-mysql for your project.

| spring-boot-starter-data-r2dbc | spring-data-r2dbc | r2dbc-spi     | r2dbc-mysql(recommended)     |
|--------------------------------|-------------------|---------------|------------------------------|
| 3.0.* and above                | 3.0.* and above   | 1.0.0.RELEASE | io.asyncer:r2dbc-mysql:1.1.0 |
| 2.7.*                          | 1.5.*             | 0.9.1.RELEASE | io.asyncer:r2dbc-mysql:0.9.7 |
| 2.6.* and below                | 1.4.* and below   | 0.8.6.RELEASE | dev.miku:r2dbc-mysql:0.8.2   |

## Supported Features
This driver provides the following features:

- [x] Unix domain socket.
- [x] Compression protocols, including zstd and zlib.
- [x] Execution of simple or batch statements without bindings.
- [x] Execution of prepared statements with bindings.
- [x] Reactive LOB types (e.g. BLOB, CLOB)
- [x] All charsets from MySQL, like `utf8mb4_0900_ai_ci`, `latin1_general_ci`, `utf32_unicode_520_ci`, etc.
- [x] All authentication types for MySQL, like `caching_sha2_password`, `mysql_native_password`, etc.
- [x] General exceptions with error code and standard SQL state mappings.
- [x] Secure connection with verification (SSL/TLS), auto-select TLS version for community and enterprise editions.
- [x] SSL tunnel for proxy protocol of MySQL.
- [x] Transactions with savepoint.
- [x] Native ping can be sent via `Connection.validate(ValidationDepth.REMOTE)` and the lightweight ping syntax `/* ping */ ...`.
- [x] Extensible, e.g. extend built-in `Codec`(s).
- [x] MariaDB `RETURNING` clause.

## Maintainer

This project is currently being maintained by [@jchrys](https://github.com/jchrys)

## Version compatibility / Integration tests states
![MySQL 5.5 status](https://img.shields.io/badge/MySQL%205.5-pass-blue)
![MySQL 5.6 status](https://img.shields.io/badge/MySQL%205.6-pass-blue)
![MySQL 5.7 status](https://img.shields.io/badge/MySQL%205.7-pass-blue)
![MySQL 8.0 status](https://img.shields.io/badge/MySQL%208.0-pass-blue)
![MySQL 8.1 status](https://img.shields.io/badge/MySQL%208.1-pass-blue)
![MySQL 8.2 status](https://img.shields.io/badge/MySQL%208.2-pass-blue)
![MariaDB 10.6 status](https://img.shields.io/badge/MariaDB%2010.6-pass-blue)
![MariaDB 10.11 status](https://img.shields.io/badge/MariaDB%2010.11-pass-blue)


In fact, it supports lower versions, in the theory, such as 4.1, 4.0, etc.

However, Docker-certified images do not have these versions lower than 5.5.0, so tests are not integrated on these versions.

## Maven

```xml

<dependency>
  <groupId>io.asyncer</groupId>
  <artifactId>r2dbc-mysql</artifactId>
  <version>1.1.0</version>
</dependency>
```

## Gradle

### Groovy DSL

```groovy
dependencies {
    implementation 'io.asyncer:r2dbc-mysql:1.1.0'
}
```

### Kotlin DSL

```kotlin
dependencies {
    // Maybe should to use `compile` instead of `implementation` on the lower version of Gradle.
    implementation("io.asyncer:r2dbc-mysql:1.1.0")
}
```

## Getting Started

Here is a quick teaser of how to use R2DBC MySQL in Java:

```java
// Notice: the query string must be URL encoded
ConnectionFactory connectionFactory = ConnectionFactories.get("r2dbcs:mysql://root:database-password-in-here@127.0.0.1:3306/r2dbc");

// Creating a Mono using Project Reactor
Mono<Connection> connectionMono = Mono.from(connectionFactory.create());
```

See [Getting Started](https://github.com/asyncer-io/r2dbc-mysql/wiki/getting-started) and [Configuration Options](https://github.com/asyncer-io/r2dbc-mysql/wiki/Configuration-Options) wiki for more information.

### Pooling

See [r2dbc-pool](https://github.com/r2dbc/r2dbc-pool).

### Simple statement

```java
connection.createStatement("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')")
    .execute(); // return a Publisher include one Result
```

### Parametrized statement

```java
connection.createStatement("INSERT INTO `person` (`birth`, `nickname`, `show_name`) VALUES (?, ?name, ?name)")
    .bind(0, LocalDateTime.of(2019, 6, 25, 12, 12, 12))
    .bind("name", "Some one") // Not one-to-one binding, call twice of native index-bindings, or call once of name-bindings.
    .add()
    .bind(0, LocalDateTime.of(2009, 6, 25, 12, 12, 12))
    .bind(1, "My Nickname")
    .bind(2, "Naming show")
    .returnGeneratedValues("generated_id")
    .execute(); // return a Publisher include two Results.
```

- All parameters must be bound before execute, even parameter is `null` (use `bindNull` to bind `null`).
- It will be using client-preparing by default, see `useServerPrepareStatement` in configuration.
- In one-to-one binding, because native MySQL prepared statements use index-based parameters, *index-bindings* will have **better** performance than *name-bindings*.

### Batch statement

```java
connection.createBatch()
    .add("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')")
    .add("UPDATE `earth` SET `count` = `count` + 1 WHERE `id` = 'human'")
    .execute(); // return a Publisher include two Results.
```

> The last `;` will be removed if and only if last statement contains ';', and statement has only whitespace follow the last `;`.

### Transactions

```java
connection.beginTransaction()
    .then(Mono.from(connection.createStatement("INSERT INTO `person` (`first_name`, `last_name`) VALUES ('who', 'how')").execute()))
    .flatMap(Result::getRowsUpdated)
    .thenMany(connection.createStatement("INSERT INTO `person` (`birth`, `nickname`, `show_name`) VALUES (?, ?name, ?name)")
        .bind(0, LocalDateTime.of(2019, 6, 25, 12, 12, 12))
        .bind("name", "Some one")
        .add()
        .bind(0, LocalDateTime.of(2009, 6, 25, 12, 12, 12))
        .bind(1, "My Nickname")
        .bind(2, "Naming show")
        .returnGeneratedValues("generated_id")
        .execute())
    .flatMap(Result::getRowsUpdated)
    .then(connection.commitTransaction());
```

## Data Type Mapping

The default built-in `Codec`s  reference table shows the type mapping between [MySQL][m] and Java data types:

| MySQL Type | Unsigned | Support Data Type |
|---|---|---|
| `INT` | `UNSIGNED` | [**`Long`**][java-Long-ref], [`BigInteger`][java-BigInteger-ref] |
| `INT` | `SIGNED` | [**`Integer`**][java-Integer-ref], [`Long`][java-Long-ref], [`BigInteger`][java-BigInteger-ref] |
| `TINYINT` | `UNSIGNED` | [**`Short`**][java-Short-ref], [`Integer`][java-Integer-ref], [`Long`][java-Long-ref], [`BigInteger`][java-BigInteger-ref], [`Boolean`][java-Boolean-ref] (Size is 1) |
| `TINYINT` | `SIGNED` | [**`Byte`**][java-Byte-ref], [`Short`][java-Short-ref], [`Integer`][java-Integer-ref], [`Long`][java-Long-ref], [`BigInteger`][java-BigInteger-ref], [`Boolean`][java-Boolean-ref] (Size is 1) |
| `SMALLINT` | `UNSIGNED` | [**`Integer`**][java-Integer-ref], [`Long`][java-Long-ref], [`BigInteger`][java-BigInteger-ref] |
| `SMALLINT` | `SIGNED` | [**`Short`**][java-Short-ref], [`Integer`][java-Integer-ref], [`Long`][java-Long-ref], [`BigInteger`][java-BigInteger-ref] |
| `MEDIUMINT` | `SIGNED/UNSIGNED` | [**`Integer`**][java-Integer-ref], [`Long`][java-Long-ref], [`BigInteger`][java-BigInteger-ref] |
| `BIGINT` | `UNSIGNED` | [**`BigInteger`**][java-BigInteger-ref], [`Long`][java-Long-ref] (Not check overflow) |
| `BIGINT` | `SIGNED` | [**`Long`**][java-Long-ref], [`BigInteger`][java-BigInteger-ref] |
| `FLOAT` | `SIGNED` / `UNSIGNED` | [**`Float`**][java-Float-ref], [`BigDecimal`][java-BigDecimal-ref] |
| `DOUBLE` | `SIGNED` / `UNSIGNED` | [**`Double`**][java-Double-ref], [`BigDecimal`][java-BigDecimal-ref]  |
| `DECIMAL` | `SIGNED` / `UNSIGNED` | [**`BigDecimal`**][java-BigDecimal-ref], [`Float`][java-Float-ref] (Size less than 7), [`Double`][java-Double-ref] (Size less than 16) |
| `BIT` | - | [**`ByteBuffer`**][java-ByteBuffer-ref], [`BitSet`][java-BitSet-ref], [`Boolean`][java-Boolean-ref] (Size is 1), `byte[]` |
| `DATETIME` / `TIMESTAMP` | - | [**`LocalDateTime`**][java-LocalDateTime-ref], [`ZonedDateTime`][java-ZonedDateTime-ref], [`OffsetDateTime`][java-OffsetDateTime-ref], [`Instant`][java-Instant-ref] |
| `DATE` | - | [**`LocalDate`**][java-LocalDate-ref] |
| `TIME` | - | [**`LocalTime`**][java-LocalTime-ref], [`Duration`][java-Duration-ref], [`OffsetTime`][java-OffsetTime-ref] |
| `YEAR` | - | [**`Short`**][java-Short-ref], [`Integer`][java-Integer-ref], [`Long`][java-Long-ref], [`BigInteger`][java-BigInteger-ref], [`Year`][java-Year-ref] |
| `VARCHAR` / `NVARCHAR` | - | [**`String`**][java-String-ref] |
| `VARBINARY` | - | [**`ByteBuffer`**][java-ByteBuffer-ref], `Blob`, `byte[]` |
| `CHAR` / `NCHAR` | - | [**`String`**][java-String-ref] |
| `ENUM` | - | [**`String`**][java-String-ref], [`Enum<?>`][java-Enum-ref] |
| `SET` | - | **`String[]`**, [`String`][java-String-ref], [`Set<String>`][java-Set-ref] and [`Set<Enum<?>>`][java-Set-ref] ([`Set<T>`][java-Set-ref] need use [`ParameterizedType`][java-ParameterizedType-ref]) |
| `BLOB`s (`LONGBLOB`, etc.) | - | [**`ByteBuffer`**][java-ByteBuffer-ref], `Blob`, `byte[]` |
| `TEXT`s (`LONGTEXT`, etc.) | - | [**`String`**][java-String-ref], `Clob` |
| `JSON` | - | [**`String`**][java-String-ref], `Clob` |
| `GEOMETRY` | - | **`byte[]`**, `Blob` |

## Statements Logging/Debugging

Use the `io.asyncer.r2dbc.mysql.QUERY` logger and the `DEBUG` log level to log statements and their bound
parameters (if it is prepared statement).

For example, in `logback.xml`:

```xml
<configuration>
    <!-- ... -->
    <logger name="io.asyncer.r2dbc.mysql" level="INFO"/> <!-- or DEBUG if necessary -->
    <logger name="io.asyncer.r2dbc.mysql.QUERY" level="DEBUG"/>
    <!-- ... -->
</configuration>
```

Note that it will print the SQL statement and all parameters, so this may be a security risk. Don't use it
in an environment with sensitive data. It should be used for debugging purposes only.

The log format may be different for server-preparing and client-preparing. This is because the actual
generated statements and commands are different in these two modes. For example, a server-preparing statement
has its statement ID that's generated by server, but client-preparing does not.

## Reporting Issues

The R2DBC MySQL Implementation uses GitHub as issue tracking system to record bugs and feature requests. 
If you want to raise an issue, please follow the recommendations below:

- Before log a bug, please search the [issue tracker](https://github.com/asyncer-io/r2dbc-mysql/issues) to see if someone has already reported the problem.
- If the issue doesn't already exist, [create a new issue](https://github.com/asyncer-io/r2dbc-mysql/issues/new).
- Please provide as much information as possible with the issue report, we like to know the version of R2DBC MySQL that you are using and JVM version.
- If you need to paste code, or include a stack trace use Markdown **&#96;&#96;&#96;** escapes before and after your text.
- If possible try to create a test-case or project that replicates the issue. Attach a link to your code or a compressed file containing your code.

## Before use

- The MySQL data fields encoded by index-based natively, get fields by an index will have **better** performance than get by column name.
- Each `Result` should be used (call `getRowsUpdated` or `map`/`flatMap`, even table definition), can **NOT** just ignore any `Result`, otherwise inbound stream is unable to align. (like `ResultSet.close` in jdbc, `Result` auto-close after used by once)
- The MySQL server does not **actively** return time zone when query `DATETIME` or `TIMESTAMP`, this driver does not attempt time zone conversion. That means should always use `LocalDateTime` for SQL type `DATETIME` or `TIMESTAMP`. Execute `SHOW VARIABLES LIKE '%time_zone%'` to get more information.
- Should not turn-on the `trace` log level unless debugging. Otherwise, the security information may be exposed through `ByteBuf` dump.
- If `Statement` bound `returnGeneratedValues`, the `Result` of the `Statement` can be called both of `getRowsUpdated` and `map`/`flatMap`.
  - If server is MariaDB 10.5.1 and above: the statement will attempt to use `RETURNING` clause, zero arguments will make the statement like `... RETURNING *`.
  - Otherwise: `returnGeneratedValues` can only be called with one or zero arguments, and `map`/`flatMap` will emit the last inserted id.
- The MySQL may be not support well for searching rows by a binary field, like `BIT` and `JSON`
  - `BIT`: cannot select 'BIT(64)' with value greater than 'Long.MAX_VALUE' (or equivalent in binary)
  - `JSON`: different MySQL may have different serialization formats, e.g. MariaDB and MySQL
- MySQL 8.0+ disables `@@global.local_infile` by default, make sure `@@local_infile` is `ON` before enable `allowLoadLocalInfileInPath` of the driver. e.g. run `SET GLOBAL local_infile=ON`, or set it in `mysql.cnf`.

## License

This project is released under version 2.0 of the [Apache License](https://www.apache.org/licenses/LICENSE-2.0).

# Acknowledgements

## Contributors

Thanks a lot for your support!

## Supports

- [R2DBC Team](https://r2dbc.io) - Thanks for their support by sharing all relevant resources around R2DBC 
  projects.

[m]: https://www.mysql.com
[java-BigDecimal-ref]: https://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html
[java-BigInteger-ref]: https://docs.oracle.com/javase/8/docs/api/java/math/BigInteger.html
[java-BitSet-ref]: https://docs.oracle.com/javase/8/docs/api/java/util/BitSet.html
[java-Boolean-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Boolean.html
[java-Byte-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Byte.html
[java-ByteBuffer-ref]: https://docs.oracle.com/javase/8/docs/api/java/nio/ByteBuffer.html
[java-Double-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Double.html
[java-Float-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Float.html
[java-Integer-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Integer.html
[java-Long-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Long.html
[java-LocalDateTime-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDateTime.html
[java-ZonedDateTime-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/ZonedDateTime.html
[java-OffsetDateTime-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/OffsetDateTime.html
[java-Instant-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/Instant.html
[java-LocalDate-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalDate.html
[java-Duration-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/Duration.html
[java-LocalTime-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/LocalTime.html
[java-OffsetTime-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/OffsetTime.html
[java-Year-ref]: https://docs.oracle.com/javase/8/docs/api/java/time/Year.html
[java-Short-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Short.html
[java-Enum-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/Enum.html
[java-String-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/String.html
[java-Set-ref]: https://docs.oracle.com/javase/8/docs/api/java/util/Set.html
[java-ParameterizedType-ref]: https://docs.oracle.com/javase/8/docs/api/java/lang/reflect/ParameterizedType.html
