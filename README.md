# DBPool : Java Database Connection Pooling

## What is DBPool?
A Java-based database connection pooling utility, supporting time-based idle timeout, statement caching, connection validation, and easy configuration using a pool manager.

### Why would I use it?

Applications using databases often need to obtain connections to the database frequently. For example, a popular website serving information from a database may need a connection for each client requesting a page using their browser. To ensure good application response time for each client, the application needs to be profiled to find the time spent performing each of its tasks. One of the most expensive database-related tasks is the initial creation of the connection. Once the connection has been made the transaction often takes place very quickly. A connection pool maintains a pool of opened connections so the application can simply grab one when it needs to, use it, and then hand it back, eliminating much of the long wait for the creation of connections.

### Licence Agreement

DBPool is available under a BSD-style licence, which can be viewed in the LICENSE.txt file.

## What are the requirements/dependencies?

* **Requirements:** DBPool 6.0 requires Java Platform 7 (also known as Java 1.7.x) or above, with support for JDBC 4.1 features. If rebuilding from the source code, [Apache Maven](http://maven.apache.org/) is recommended as a build tool.
* **Dependencies:** [SLF4J](http://www.slf4j.org/)

## Where can I find more detailed information?

For full documentation see the [DBPool webpage](http://www.snaq.net/java/DBPool/).
