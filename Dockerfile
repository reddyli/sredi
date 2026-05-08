# syntax=docker/dockerfile:1.6

# ---------- Stage 1: build the fat jar ----------
FROM eclipse-temurin:23-jdk AS build
WORKDIR /app

# Pre-copy wrapper + build scripts so dependency resolution is cached
COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
RUN chmod +x ./gradlew && ./gradlew --no-daemon -q help

# Copy sources and build
COPY src ./src
RUN ./gradlew --no-daemon bootJar -x test

# ---------- Stage 2: runtime ----------
FROM eclipse-temurin:23-jre
WORKDIR /app
COPY --from=build /app/build/libs/*.jar /app/sredi.jar

# Default client port + mesh port (mesh = client + 10000).
# Override with --port at runtime; remember to publish the matching mesh port too.
EXPOSE 6379 16379

ENTRYPOINT ["java", "-jar", "/app/sredi.jar"]
