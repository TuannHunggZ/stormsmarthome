FROM maven:3.9.11-eclipse-temurin-17-alpine
WORKDIR /app
COPY pom.xml .
COPY . .
CMD ["mvn", "spring-boot:run"]