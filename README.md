## Volleyball Tournament Scoreboard

### Project Overview
Volleyball tournament score collection and standings calculation in real-time.
![image](images/courts.png)

---
### Solution Overview
Streaming **Scorecard** scores to _Kafka_ and consuming scores for **Scoreboard** visualization using _WebSockets_. Also consuming scores by _Spark Streaming_ for real-time **Standings**.
![image](images/architecture.png)

---
### Development Stack
- Scala and sbt (compiler and build tool)
- Apache Kafka Java Client library (producer and consumer apis)
- Akka-Http Scala library (Http and WebSocket server)
- Apache Spark Streaming (tournament standings)
- Apache Zeppelin Notebooks (further analysis of tournament) 
- D3.js (browser visualization)

---
### Solution - Score Collection
**Scorecard** data is collected from volleyball courts and posted on **Scoreboard**.
![image](images/scorecard.png)

---
### Solution - Teams Standing
**Standings** are calculated in real-time as soon as the latest scores arrive.
![image](images/standings.png)

---
### Code structure (Classes and Objects)
![image](images/package.png)
