FROM maven:3.5-jdk-11 as build-model

# build the model SDK
WORKDIR /model
# copy the Project Object Model file and check dependencies
COPY home-architekt-model/pom.xml ./pom.xml
# copy source files
COPY home-architekt-model/src ./src
# build for release
RUN mvn package --fail-never && cp target/home-architekt-model-*.jar /

FROM maven:3.5-jdk-11 as build-producer

# build the producer
WORKDIR /producer
# copy the Project Object Model file and check dependencies
COPY home-architekt-producer/pom.xml ./pom.xml
COPY --from=build-model /home-architekt-model-*.jar ./model.jar
RUN mvn org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file -Dfile=./model.jar
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
COPY --from=build-producer /producer.jar .

# set the startup command to run your binary
CMD ["java", "-jar", "/app/producer.jar"]