FROM debian:bookworm-slim AS builder

ARG TELEGRAM_BOT_API_COMMIT=e3e9dd8e5b3d7ab8537cd5a10dc31d5ffa8f82d1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates cmake g++ git gperf make libssl-dev zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --recursive https://github.com/tdlib/telegram-bot-api.git /src \
    && cd /src \
    && git checkout "$TELEGRAM_BOT_API_COMMIT" \
    && git submodule update --init --recursive

RUN cmake -S /src -B /src/build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_INSTALL_PREFIX=/opt/telegram-bot-api \
    && cmake --build /src/build --target install --parallel 2

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates curl libssl3 libstdc++6 zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/telegram-bot-api/bin/telegram-bot-api /usr/local/bin/telegram-bot-api

EXPOSE 8081

ENTRYPOINT ["/usr/local/bin/telegram-bot-api"]
