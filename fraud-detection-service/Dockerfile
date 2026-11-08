FROM maven:3.8.8-openjdk-11-slim AS builder
WORKDIR /workspace
COPY pom.xml .
COPY common-contracts common-contracts
COPY fraud-detection-service fraud-detection-service
RUN mvn clean package -pl common-contracts,fraud-detection-service -am -DskipTests

FROM eclipse-temurin:11-jre-focal
WORKDIR /app
COPY --from=builder /workspace/fraud-detection-service/target/*.jar app.jar
EXPOSE 8084
ENTRYPOINT ["java", "-jar", "app.jar"]

