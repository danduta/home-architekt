# our base build image
FROM maven:3.5-jdk-11 as maven

# build the model SDK
WORKDIR /model
# copy the Project Object Model file
COPY home-architekt-model/pom.xml ./pom.xml
RUN mvn verify clean --fail-never
# copy source files
COPY home-architekt-model/src ./src
# build for release
RUN mvn install && cp target/home-architekt-model-*.jar /

# build the producer
WORKDIR /producer
# copy the Project Object Model file
COPY home-architekt-producer/pom.xml ./pom.xml
RUN mvn verify clean --fail-never
# copy source files
COPY home-architekt-producer/src ./src
# build for release
RUN mvn package && cp target/home-architekt-producer-*-jar-with-dependencies.jar /producer.jar

# smaller, final base image
FROM openjdk:11-jre

# set deployment directory
WORKDIR /app

# copy dataset
COPY home-architekt-producer/dataset.csv dataset.csv

# copy over the built artifact from the maven image
COPY --from=maven /home-architekt-model-*.jar .
COPY --from=maven /producer.jar .

# set the startup command to run your binary
CMD ["java", "-jar", "/app/producer.jar"]