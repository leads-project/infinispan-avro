# Infinispan-avro

### Description 
This project offers an [Infinispan](http://infinspan.org) support for [Apache Avro](https://avro.apache.org/), providing the ability to store and query Avro defined types with the help of Infinispan [query DSL](http://infinispan.org/docs/7.2.x/user_guide/user_guide.html#_infinispan_s_query_dsl).

This project is also intended to be used in conjunction with the Infinispan support for [Apache Gora](https://github.com/leads-project/gora-infinispan), in order to execute Hadoop map-reduce tasks on top of Infinispan.

### Requirements
infinispan-8.0.0-SNAPSHOT

### Installation 
This project is based upon Maven. It requires to install first the latest snapshot version of Infinispan, available at the following [address](https://github.com/infinispan/infinispan).

### Usage
Storing, retrieving and Querying Avro defined types requires to start HotRod server, enabling the infinispan-avro-server module. Then, the client side should depend on the infinispan-avro-hotorod module. To build an Avro defined type, please refer to the Apache Avro documentation on the [compiler](https://avro.apache.org/docs/1.7.7/gettingstartedjava.html). 

## Code Sample
Let Employee be an Avro defined type. The following code illustrates how to store and query Employee instances.

```java

import example.avro.Employee;
import org.infinispan.avro.hotrod.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
...

Employee employee = new Employee();
employee.setName("Pierre");
employee.setSalary(5000);
employee.setSsn("12478");
employee.setDateOfBirth((long) 200991);

remotecache.put(1,employee);

QueryFactory qf = Search.getQueryFactory(remoteCache1);

Query query = qf.from(Employee.class)
                .having("ssn").eq("12478").toBuilder()
                .build();
List<Employee> list = query.list();
assertNotNull(list);
assertEquals(1, list.size());

```

Every field of an Avro defined type with a (String converted) length smaller than 1000 bytes can be queried through the DSL. As with original HotRod queries, a query is executed locally at the server that receives it. 

