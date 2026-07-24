# ─── Stage 1: Build ────────────────────────────────────────────
FROM maven:3.9-eclipse-temurin-21-alpine AS builder
WORKDIR /build

# Maven dependency cache (speeds up rebuilds)
COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 \
    mvn dependency:go-offline -q 2>/dev/null || true

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 \
    mvn package -DskipTests -q

# ─── Stage 2: Runtime ──────────────────────────────────────────
# Minimal image: ~85MB vs ~300MB of a full JDK
FROM eclipse-temurin:21-jre-alpine AS runtime

WORKDIR /app

# Non-root user — mandatory practice in banking environments
RUN addgroup -S bank && adduser -S -G bank bank

# Copy only the final JAR
COPY --from=builder --chown=bank:bank /build/target/banking-saga-poc-*.jar app.jar

USER bank

# JVM optimizations for containers:
# -XX:+UseContainerSupport  → detects the container's CPU/memory limits
# -XX:MaxRAMPercentage=75   → uses 75% of the available RAM for the heap
ENV JAVA_OPTS="-XX:+UseContainerSupport \
               -XX:MaxRAMPercentage=75 \
               -XX:+OptimizeStringConcat \
               -Djava.security.egd=file:/dev/./urandom"

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost:8080/transfers/health || exit 1

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]

# ─── GraalVM Native variant (optional) ─────────────────────────
# For a native build (startup ~50ms, memory ~50MB):
#
# FROM ghcr.io/graalvm/native-image:21 AS native-builder
# COPY --from=builder /build/target/banking-saga-poc-*.jar app.jar
# RUN native-image -jar app.jar -o app --no-fallback \
#     --initialize-at-build-time=org.slf4j \
#     --enable-url-protocols=http
#
# FROM debian:bookworm-slim
# COPY --from=native-builder /app/app /app/app
# ENTRYPOINT ["/app/app"]
