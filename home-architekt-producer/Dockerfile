# our base build image
FROM maven:3.5-jdk-11 as maven

WORKDIR /app

# copy the Project Object Model file
COPY ./pom.xml ./pom.xml

RUN mvn verify clean --fail-never

# copy your other files
COPY ./src ./src

# build for release
RUN mvn package && cp target/home-architekt-producer-*-jar-with-dependencies.jar app.jar

# smaller, final base image
FROM openjdk:11-jre

# set deployment directory
WORKDIR /app

# copy dataset
COPY dataset.csv dataset.csv

# copy over the built artifact from the maven image
COPY --from=maven /app/app.jar ./app.jar

# set the startup command to run your binary
CMD ["java", "-jar", "/app/app.jar"]