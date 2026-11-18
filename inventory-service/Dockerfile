FROM maven:3.9.6-eclipse-temurin-21-jammy AS builder
WORKDIR /workspace
COPY pom.xml .
COPY common-contracts common-contracts
COPY inventory-service inventory-service
RUN mvn clean package -pl common-contracts,inventory-service -am -DskipTests

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=builder /workspace/inventory-service/target/*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]

