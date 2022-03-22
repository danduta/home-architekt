# our base build image
FROM maven:3.5-jdk-11 as maven

WORKDIR /app

# copy the Project Object Model file
COPY ./pom.xml ./pom.xml

RUN mvn verify clean --fail-never

# copy your other files
COPY ./src ./src

# build for release
RUN mvn package && cp target/home-architekt-service-*-jar-with-dependencies.jar app.jar

# smaller, final base image
FROM docker.io/bitnami/spark:3

# set deployment directory
WORKDIR /app

# copy over the built artifact from the maven image
COPY --from=maven /app/app.jar ./app.jar

# set the startup command to run your binary
#CMD ["./bin/spark-submit", "--class", "io.github.danduta.service.Application", "--master", "spark://localhost:7077", "app.jar"]
CMD spark-submit --class io.github.danduta.service.Application --master spark://spark-master:7077 ./app.jar
