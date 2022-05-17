FROM folioci/alpine-jre-openjdk17:latest

ENV VERTICLE_FILE mod-meta-storage-server-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

# Copy your fat jar to the container
COPY server/target/${VERTICLE_FILE} ${VERTICLE_HOME}/${VERTICLE_FILE}

COPY server/target/compiler/*.jar ${VERTICLE_HOME}/compiler/

# Expose this port locally in the container.
EXPOSE 8081

ENV JAVA_OPTIONS "--module-path=compiler/ --upgrade-module-path=compiler/compiler.jar:compiler/compiler-management.jar -XX:+UnlockExperimentalVMOptions -XX:+EnableJVMCI"
