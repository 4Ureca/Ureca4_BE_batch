FROM eclipse-temurin:17-jre

WORKDIR /app

ARG JAR_FILE=build/libs/*.jar
COPY ${JAR_FILE} app.jar

ENTRYPOINT ["java","-Xms512m","-Xmx2048m","-XX:+UseG1GC","-jar","app.jar"]