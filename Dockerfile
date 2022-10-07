FROM openjdk:8u332-jdk-bullseye AS linux-builder
ENV DEBIAN_FRONTEND=noninteractive

ADD . /build
WORKDIR /build

RUN apt-get update \
  && apt-get install -y curl gnupg \
  && apt-get update \
  && apt-get install -y \
    g++ \
    make \
    nodejs \
    npm \
    python \
    python3-distutils \
    tzdata \
  && apt-get autoremove -y \
  && apt-get clean -y \
  && rm -rf /var/lib/apt/lists/*
RUN npx @bazel/bazelisk build --copt "-O3" //zetasql/local_service:libremote_server.so //zetasql/local_service:remote_server_executable

FROM alpine
ARG TARGETPLATFORM
COPY --from=linux-builder /build/bazel-bin/zetasql/local_service/libremote_server.so ${TARGETPLATFORM}/libremote_server.so
COPY --from=linux-builder /build/bazel-bin/zetasql/local_service/remote_server_executable ${TARGETPLATFORM}/remote_server_executable
