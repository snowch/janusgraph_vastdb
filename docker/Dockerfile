FROM openjdk:17-jdk-slim

WORKDIR /app
COPY target/janusgraph-vastdb-*.jar app.jar

EXPOSE 8182

ENTRYPOINT ["java", "-jar", "app.jar"]
