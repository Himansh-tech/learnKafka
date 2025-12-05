# learnKafka

Day 1: 05/12/25
* Suppose we have ecommerce store which have many microservice like order, cart, payment etc etc, so If there are very few users then microservcies can directly call each other(for communication) whenever needed but, as the user increases we can't rely on this way of talking(communication) between microservives, as if one servcie is down and still other sends some thing then it will be lost in between apart from this direct communication between services casues tight coupling, less scalable.

<img width="613" height="198" alt="image" src="https://github.com/user-attachments/assets/0c3d425e-758b-4678-a26b-d79861bb62f4" />

Even if we provide infinte CPU/RAM to services it can't solve problem like this
<img width="623" height="636" alt="image" src="https://github.com/user-attachments/assets/ef8b19a4-a954-46f0-acf5-fcd679dcffd0" />
