# learnKafka

Day 1: 05/12/25
* Suppose we have ecommerce store which have many microservice like order, cart, payment etc etc, so If there are very few users then microservcies can directly call each other(for communication) whenever needed but, as the user increases we can't rely on this way of talking(communication) between microservives, as if one servcie is down and still other sends some thing then it will be lost in between apart from this direct communication between services casues tight coupling, less scalable.

<img width="613" height="198" alt="image" src="https://github.com/user-attachments/assets/0c3d425e-758b-4678-a26b-d79861bb62f4" />

Even if we provide infinte CPU/RAM to services it can't solve problem like this
<img width="623" height="636" alt="image" src="https://github.com/user-attachments/assets/ef8b19a4-a954-46f0-acf5-fcd679dcffd0" />

===================================================================================

Some gpt Q & A
📌 SHORT NOTES – Microservices, Sync vs Async, Kafka, DB Connections

1) ❓ Why does direct communication between microservices fail at scale?
   ✅ Because synchronous service-to-service calls cause:
      - Tight coupling (one service depends on others being alive right now)
      - Cascading failures (one slow service slows or breaks the whole chain)
      - Retry storms under high traffic
      - Very high latency when many services call each other
      - Complexity explosion as services grow (N^2 connections)
      - Inability to handle traffic spikes smoothly
   Kafka/Queues solve this by decoupling services and making work asynchronous.

2) ❓ If we had infinite CPU/RAM, would direct communication work?
   ❌ No.
   Even with infinite computation, real-world problems remain:
      - Network failures
      - Services restarting during deployment
      - External APIs being slow
      - Backpressure (producers faster than consumers)
      - Need for durable history and reprocessing
   Kafka solves these reliability/time-decoupling issues, not just performance.

3) ❓ Why does a database have a connection limit? Can it be infinite?
   ❌ DB connections cannot be infinite because each connection uses:
      - Memory (session buffers)
      - A thread or process
      - OS resources (file descriptors)
      - CPU time for context switching
   Too many connections actually slows the DB.
   ✔ Use connection pooling, replicas, and sharding instead.

4) ❓ Why do services use synchronous communication? Can they be asynchronous?
   ✔ Synchronous (HTTP/gRPC) is used where user needs immediate response:
      - Login
      - Place Order (basic confirmation)
      - View details
   ✔ Asynchronous (Kafka) is used for internal work:
      - Payment processing
      - Inventory updates
      - Sending emails
      - Analytics
   Using async prevents blocking and improves scalability.

5) ❓ Does Kafka ensure synchronous calls?
   ❌ No. Kafka is fully asynchronous.
   Kafka does NOT wait for consumers.
   Kafka = “store event safely, process later”.

6) ❓ So how do modern systems work with sync + async?
   ✔ Sync at the edges:
      - User → API → service returns response
   ✔ Async inside:
      - Service publishes event to Kafka
      - Other services process later
      - Ensures no message loss and no blocking

7) ❓ Summary (super short)
   - Sync = for user response.
   - Async = for heavy internal workflows.
   - Kafka = reliable async communication.
   - DB limits = to protect performance and resources.
   - Direct microservice calls fail at scale due to coupling, latency, and failures.

===================================================================================

✔ Events are messages that one service sends to another.
In Kafka, an event = a message describing something that happened.
Kafka event is usually sent as key–value JSON, and Kafka also adds or stores some metadata like when it was created, who created it.
Examples:
OrderCreated
PaymentSuccess
CartUpdated

===================================================================================

✔ Producer is the service that creates events and sends them to Kafka.
Producer → writes messages into a topic.

Example:
Order Service = Producer
It produces an event → OrderCreated → sends to Kafka topic.

===================================================================================

 ✔ Consumer

A service that reads messages from Kafka.
Consumers subscribe to specific topic, and whatever events are put into it they receive that.
❌ Wrong idea

“Kafka notifies the consumer when a new event appears.”

✅ Correct idea:
Consumers keep pulling (polling) Kafka for new messages at regular intervals.
Kafka is a pull-based system → Consumers ask Kafka,
Kafka does NOT push messages to consumers.
Kafka does NOT push notifications to consumers.
Consumers poll Kafka, but it is long-polling, which is efficient.

Pull is used because:
- Consumers control their speed
- No risk of overload
- Better scaling
- Easier retry and offset management

Push systems exist (RabbitMQ, Webhooks, MQTT), but cannot match Kafka’s scale.

===================================================================================

✔ Topic

A named channel where events/messages are stored.
A Kafka topic is more like a folder or category where events of the same type are stored.
Example: "order-events".

✅ Perfect explanation (easy to remember)
📌 Topic = Category of events
Just like:
Emails are grouped into folders (Inbox, Promotions, Updates)
Files are grouped into folders (Images, Videos, Documents)
Kafka groups events into topics.

Examples:
order-events
payment-events
inventory-updates
user-notifications

Each topic stores only that type of event.

❌ Topic is NOT a database
Here is the difference:

Database
Stores tables, rows, columns
Used for CRUD data
Data is updated or deleted
Queryable (SQL)

Kafka Topic
Stores events in a log sequence
Data is mostly append-only
Messages are not updated
Not queryable like SQL
Messages expire after retention time

===================================================================================

kafka is not replacement to database, instead take it as tool that enables chain reaction where one event triggers multiple action, including creating other event or updatind database.
also it enables real time analytics(like live driver location in uber). kafak is made for application with very large data

===================================================================================

# Kafka Partitions – Short Notes

A partition is a sub-division inside a Kafka topic.  
Each partition stores messages in an ordered, append-only sequence.  
Every message inside a partition has an offset (0,1,2…).

## Notebook Example:
Think of a topic as a notebook.
- Topic = Notebook
- Partition = Page inside the notebook
- Message/Event = Line on that page
- Offset = Line number

Ordering is guaranteed only within a single partition (page).

## Why partitions exist:
1. To scale writes (multiple partitions = parallel writing)
2. To scale consumers (each partition can be read by one consumer)
3. To provide fault tolerance (replication across brokers)

## How messages go to partitions:
- If a key is provided → Kafka hashes the key and sends all messages 
  with the same key to the same partition (keeps order).
- If no key is provided → Kafka uses round-robin to distribute messages.

## Example:
Topic: order-events
Partitions:
  Partition-0: offset 0,1,2...
  Partition-1: offset 0,1,2...
  Partition-2: offset 0,1,2...
Different consumers can read different partitions in parallel.

More partitions = more throughput and more parallelism.


example if you have topic "orders" for ecommerce => then you can have partitions like US orders, Africa orders e

===================================================================================

# Kafka Topic, Partitions, Consumer Groups, and Instances – Simple Notes

## 1. Topic and Partitions
A Kafka topic is like a big notebook where events of one category are stored.
Since one notebook page will get full or slow, we divide it into many pages.

- Topic = Notebook (example: "orders")
- Partition = Page inside notebook (Partition-0, Partition-1, Partition-2)
- Message/Event = Line written on that page
- Offset = Line number on that page

Ordering is guaranteed only within a single page (partition).

You can decide which page gets which type of data by using a key.
Example:
Key "US" → always goes to Partition-0
Key "AFRICA" → always goes to Partition-1
Key "EUROPE" → always goes to Partition-2

If US orders are 100x more, you add more partitions for US so load spreads.


## 2. Consumers and Consumer Groups
A Consumer is just a running copy of a microservice that reads data from Kafka.

But one partition can be read by only ONE consumer at a time (to keep ordering).

To scale reading, we do NOT create new microservices.
We create multiple copies (instances) of the SAME microservice.
All these instances join the same consumer group.

Consumer Group = A team of consumer instances reading from the same topic.

Kafka divides partitions across the team:
- Partition-0 → instance #1
- Partition-1 → instance #2
- Partition-2 → instance #3

If one instance dies, Kafka gives its work to another instance.


## 3. How to Create Multiple Instances of the Same Microservice
A microservice is just code that runs as a program.

To create more instances:
1. Run the same application multiple times on different ports.
   Example: order-service on 8081, 8082, 8083.

2. Using Docker:
   Run the same image 3 times → 3 containers.

3. Using Kubernetes (real-world way):
   replicas: 3  → automatically creates 3 pods of the same service.

All instances:
- Have the same code
- Use the same consumer group ID
- Kafka treats them as workers sharing the load

===================================================================================

Kafka Broker:
- A Kafka server that stores data (events) inside partitions.
- Producers send events to brokers.
- Consumers read events from brokers.
- A Kafka cluster is made of multiple brokers.
- Data is spread across brokers for reliability and performance.

Kafka keeps data in brokers even after consumers read it.
Consumption does NOT delete the event.
Only retention rules (time or size) decide when old data is removed.
Consumers simply move their offset (bookmark).
Kafka’s storage model enables replay, fault tolerance, and multiple consumers.

===================================================================================


