FROM sbtscala/scala-sbt:eclipse-temurin-11.0.17_8_1.8.2_2.13.10 as builder

RUN apt-get update && apt-get install -y \
    curl \
    python3 \
    python3-dev \
    python3-setuptools \
    wget \
    procps \
    python3-pip

RUN python3 -m pip install -U tox build

ENV THRIFT_VERSION 0.13.0

# Install thrift
RUN curl -sSL "http://archive.apache.org/dist/thrift/$THRIFT_VERSION/thrift-$THRIFT_VERSION.tar.gz" -o thrift.tar.gz \
       && mkdir -p /usr/src/thrift \
       && tar zxf thrift.tar.gz -C /usr/src/thrift --strip-components=1 \
       && rm thrift.tar.gz \
       && cd /usr/src/thrift \
       && ./configure  --without-python --without-cpp \
       && make \
       && make install \
       && cd / \
       && rm -rf /usr/src/thrift

RUN mkdir /build

COPY . /build

WORKDIR /build

RUN sbt "project service" clean assembly

FROM eclipse-temurin:11-jre-jammy

COPY --from=builder /build/service/target/scala-2.12/service.jar /app/service.jar
COPY bigtable-online-impl-assembly-0.1.0-SNAPSHOT.jar /app/bigtable-online-impl-assembly-0.1.0-SNAPSHOT.jar
COPY service/src/main/resources/config.json /app/config.json

WORKDIR /app

CMD ["java", "-jar", "service.jar", "run", "ai.chronon.service.WebServiceVerticle", "-Dserver.port=9000", "-conf", "/app/config.json"]
