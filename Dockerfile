FROM ibm-semeru-runtimes:open-17.0.9_9-jre-focal
EXPOSE 8080
ARG JAR_FILE
ADD $JAR_FILE /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar", "--spring.config.additional-location=/config/"]
