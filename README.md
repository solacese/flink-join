# Example Flink Job plus Test Data Generator
There's two subprojects:
- a simple test data generator which publishes messages with random keys to two different topics
- a Flink job which perfomed time-windowed merges for messages with identical keys

Build the projects with `mvn package` to create the jar files. (Yep, Maven is required, as well as a JDK >= 11).
There's a readme file in each subproject.
