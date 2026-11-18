FROM maven:3.9.6-eclipse-temurin-21-jammy AS builder
WORKDIR /workspace
COPY pom.xml .
COPY common-contracts common-contracts
COPY order-service order-service
RUN mvn clean package -pl common-contracts,order-service -am -DskipTests

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=builder /workspace/order-service/target/*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]

