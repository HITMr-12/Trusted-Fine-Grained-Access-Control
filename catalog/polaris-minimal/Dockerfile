FROM eclipse-temurin:21-jdk AS builder
WORKDIR /src
COPY src/ ./src/
RUN mkdir -p /out \
    && find src -name '*.java' -print0 \
       | xargs -0 javac --add-modules jdk.httpserver -d /out

FROM eclipse-temurin:21-jre
WORKDIR /opt/polaris-fgac
COPY --from=builder /out/ /opt/polaris-fgac/classes/
EXPOSE 8181
ENTRYPOINT ["java", "--add-modules", "jdk.httpserver", "-cp", "/opt/polaris-fgac/classes", "org.apache.polaris.fgac.MinimalPolarisFgac"]
