# Лабораторна робота № 4

Потокова обробка енергетичних даних з транзакційною семантикою в Apache Kafka Streams

# Тема:

Потокова обробка енергетичних даних з транзакційною семантикою в Apache Kafka Streams

# Мета:

Розробити систему потокової обробки енергетичних даних з використанням
Apache Kafka Streams, що забезпечує транзакційну семантику exactly-once та
інтеграцію з Apache Cassandra для збереження результатів обробки.

Docker containers:
![Image alt](./pictures/docker-compose.png)

Producer:
![Image alt](./pictures/producer.png)

Kafka:
![Image alt](./pictures/kafka.png)

Stream proccessor:
![Image alt](./pictures/stream-processor.png)

Cassandra tables:
![Image alt](./pictures/charging-event-log.png)
![Image alt](./pictures/station-utilization-state.png)

Event sourcing:
![Image alt](./pictures/event-sourcing.png)
