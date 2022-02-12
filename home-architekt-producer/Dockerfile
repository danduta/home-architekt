# our base build image
FROM maven:3.5-jdk-8 as maven

WORKDIR /app

# copy the Project Object Model file
COPY ./pom.xml ./pom.xml

# copy your other files
COPY ./src ./src

# build for release
RUN mvn dependency:go-offline
RUN mvn package && cp target/home-architekt-producer-*-jar-with-dependencies.jar app.jar


# smaller, final base image
FROM openjdk:8u171-jre-alpine


# set deployment directory
WORKDIR /app

# copy dataset
COPY dataset.csv dataset.csv

# copy over the built artifact from the maven image
COPY --from=maven /app/app.jar ./app.jar

# set the startup command to run your binary
CMD ["java", "-jar", "/app/app.jar"]