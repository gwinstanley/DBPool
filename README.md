# DBPool : Java Database Connection Pooling

## What is DBPool?
A Java-based database connection pooling utility, supporting time-based expiry, statement caching, connection validation, and easy configuration using a pool manager.

### Why would I use it?

Applications using databases often need to obtain connections to the database frequently. For example, a popular website serving information from a database may need a connection for each client requesting a page using their browser. To ensure good application response time for each client, the application needs to be profiled to find the time spent performing each of its tasks. One of the most expensive database-related tasks is the initial creation of the connection. Once the connection has been made the transaction often takes place very quickly. A connection pool maintains a pool of opened connections so the application can simply grab one when it needs to, use it, and then hand it back, eliminating much of the long wait for the creation of connections.

### Licence Agreement

DBPool is available under a BSD-style licence, which can be viewed in the LICENSE.txt file.

## What are the requirements/dependencies?

The JDBC specification has changed out of step with the various Java Platform releases. Many pooling libraries solve this by using dynamic method resolution which allows a single codebase to work for all platforms, but at the cost of performance. DBPool aims to maintain its high-performance approach, but as a result you'll need to check carefully which version best suits your Java platform. It is highly recommended to use the most recent stable release possible, for reasons of performance, reliability, and features.

* **Requirements:** Java 1.6.x or above, supporting JDBC 3.0 or later. [Apache Maven](http://maven.apache.org/) is recommended to build the source code.
* **Dependencies:** [Apache Commons Logging](http://commons.apache.org/logging/)

## Where can I find more detailed information?

For full documentation see the [DBPool webpage](http://www.snaq.net/java/DBPool/).
