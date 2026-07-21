FROM sbtscala/scala-sbt:eclipse-temurin-8u352-b08_1.8.2_2.13.10 AS builder

WORKDIR /src

COPY tibia-bot/ /src/

RUN sbt -batch docker:stage


FROM eclipse-temurin:8-jre

COPY --from=builder /src/target/docker/stage/opt /opt

WORKDIR /opt/docker

ENTRYPOINT ["/bin/sh", "-c", "exec $(find /opt/docker/bin -maxdepth 1 -type f ! -name '*.bat' | head -n 1)"]
