FROM sbtscala/scala-sbt:eclipse-temurin-8u352-b08_1.8.2_2.13.10 AS builder

WORKDIR /src

COPY tibia-bot/ /src/

RUN sbt -batch "Universal / stage"


FROM eclipse-temurin:8-jre

COPY --from=builder /src/target/universal/stage /opt/app

WORKDIR /opt/app

RUN chmod +x /opt/app/bin/violent-bot-dedicated

ENTRYPOINT ["/opt/app/bin/violent-bot-dedicated"]
