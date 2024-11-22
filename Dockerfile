# maven build
FROM eclipse-temurin:17 AS build

WORKDIR /app

COPY mvnw .
COPY .mvn .mvn
COPY .git .git
COPY pom.xml .
COPY src src

RUN --mount=type=cache,target=/root/.m2 ./mvnw --batch-mode install -DskipTests
# extract might change with Spring Boot 3.3?
RUN java -Djarmode=layertools -jar /app/target/*.jar extract

# docker image
FROM eclipse-temurin:17-jre
RUN useradd app
USER app
WORKDIR /app
ENV OTEL_JAVAAGENT_ENABLED=false
# changes with Spring Boot 3.3
ENTRYPOINT ["java", "org.springframework.boot.loader.launch.JarLauncher"]
COPY --from=build /app/dependencies/ ./
COPY --from=build /app/spring-boot-loader/ ./
COPY --from=build /app/snapshot-dependencies/ ./
COPY --from=build /app/application/ ./
