# Example Flink Job plus Test Data Generator
There's two subprojects:
- a simple test data generator which publishes messages with random keys to two (or three or four) different topics
- Flink job examples which perform time-windowed merges for messages with identical keys. There's a join-based job (merges two streams) and a aggregate-based one (should work with more than two input streams).

Build the projects with `mvn package` to create the jar files. (Yep, Maven is required, as well as a JDK >= 11).
There's a readme file and Maven `pom.xml` in each subproject.
