FROM maven:3.5-jdk-11 as build-model

# build the model SDK
WORKDIR /model
# copy the Project Object Model file and check dependencies
COPY home-architekt-model/pom.xml ./pom.xml
# copy source files
COPY home-architekt-model/src ./src
# build for release
RUN mvn package --fail-never && cp target/home-architekt-model-*.jar /

FROM maven:3.5-jdk-11 as build-service

# build the service
WORKDIR /service
# copy the Project Object Model file and check dependencies
COPY home-architekt-outlier-detector/pom.xml ./pom.xml
COPY --from=build-model /home-architekt-model-*.jar ./model.jar
RUN mvn org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file -Dfile=./model.jar
# copy source files
COPY home-architekt-outlier-detector/src ./src
# build for release
RUN mvn package && cp target/home-architekt-outlier-detector-*-jar-with-dependencies.jar /service.jar

# smaller, final base image
FROM docker.io/bitnami/spark:3

# set deployment directory
WORKDIR /app

# copy over the built artifact from the maven image
COPY --from=build-service /service.jar .

# set the startup command to run your binary
CMD spark-submit --class io.github.danduta.app.SparkOutlierDetector --master spark://10.40.129.201:7077 ./service.jar
