FROM gcr.io/bazel-public/bazel:latest as builder

RUN apt install -y wget

RUN wget https://archive.apache.org/dist/thrift/0.13.0/thrift-0.13.0.tar.gz && \
    tar -xvf thrift-0.13.0.tar.gz && \
    cd thrift-0.13.0 && \
    sudo ./configure && \
    sudo make install

RUN mkdir /build

COPY . /build

WORKDIR /build

RUN bazel build //service:service-exec_deploy.jar

FROM eclipse-temurin:8-jre-alpine

COPY --from=builder /build/bazel-bin/service/service-exec_deploy.jar /app/service-exec_deploy.jar
COPY bigtable-online-impl-assembly-0.1.0-SNAPSHOT.jar /app/bigtable-online-impl-assembly-0.1.0-SNAPSHOT.jar
COPY service/src/main/resources/config.json /app/config.json

WORKDIR /app

CMD ["java", "-jar", "service-exec_deploy.jar", "run", "ai.chronon.service.WebServiceVerticle", "-Dserver.port=9000", "-conf", "/app/config.json"]
