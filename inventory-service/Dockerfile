FROM maven:3.8.8-openjdk-11-slim AS builder
WORKDIR /workspace
COPY pom.xml .
COPY common-contracts common-contracts
COPY inventory-service inventory-service
RUN mvn clean package -pl common-contracts,inventory-service -am -DskipTests

FROM eclipse-temurin:11-jre-focal
WORKDIR /app
COPY --from=builder /workspace/inventory-service/target/*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]

