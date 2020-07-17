# mdbconverter

#### Table of Contents

1. [Application Description - What does the application do?](#application-description)
2. [Setup](#setup)
3. [Usage](#usage)
4. [Limitations - OS compatibility, etc.](#limitations)
5. [Development - Guide for contributing to the application](#development)
    * [Contributors - List of application contributors](#contributors)

## Application description

The application allows you to convert a microsoft access database (mdb) file into a MySQL database. Based on the
available data in the mdb file, all tables will be created, data will be imported and constraints will be added.
Before the import is made, the database will be cleared.

## Setup

In src/main/resources/config.properties.template there is a sample configuration file. It can be provided to the
application with the '--config config.properties' argument. It is also possible to prove part of the configuration
via this config, and part with command line arguments.
Make sure you have a running MySQL server with an empty database and an user able to connect to it. This users must
be able to manage tables, constraints and data.

## Usage

The application can be started with the following command:

```java -jar target/mdbconverter-0.1-SNAPSHOT.jar --config config.properties
```

One need to provide MySQL url, username and password and MDB file. This can be done via the config file, the individual
arguments or a combination of both. When using '--config', make sure it is the first argument.

Possible arguments:

##### -h | --help:
Show this help
##### -c <file> | --config <file>
Config file
##### --mysqlurl <url>
MySQL connect URL. Example: jdbc:mysql://localhost/<database>?useUnicode=true&characterEncoding=UTF-8&useSSL=false
##### --mysqluser <user>
MySQL user to connect with to the database
##### --mysqlpassword <password>
MySQL password to connect with to the database. WARNING: this is not secure, use config file instead.
##### --mdb <file>
MDB file to convert to MySQL

## Limitations

Works with Java 8.

Works with versions of MySQL from 5.6.

## Development

Feel free to report a bug or submit a pull request with a fix.

### Contributors

View the full list of contributors on [Github](https://github.com/wimvr/mdbconverter/graphs/contributors).
