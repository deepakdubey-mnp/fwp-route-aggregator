FROM eclipse-temurin:25-jre-noble

WORKDIR /app

COPY build/libs/*-SNAPSHOT.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]