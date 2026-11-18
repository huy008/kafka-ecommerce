FROM maven:3.9.6-eclipse-temurin-21-jammy AS builder
WORKDIR /workspace
COPY pom.xml .
COPY common-contracts common-contracts
COPY payment-service payment-service
RUN mvn clean package -pl common-contracts,payment-service -am -DskipTests

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=builder /workspace/payment-service/target/*.jar app.jar
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]

