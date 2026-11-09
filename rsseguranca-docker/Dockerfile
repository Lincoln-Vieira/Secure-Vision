FROM rust:latest 

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    libgstreamer-plugins-bad1.0-dev \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    gstreamer1.0-tools \
    gstreamer1.0-x \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Copia os arquivos de configuração e o código fonte
COPY Cargo.toml ./
COPY src ./src
COPY assets ./assets

# Compila o projeto em modo release com o Rust atualizado
RUN cargo build --release

EXPOSE 3000

CMD ["target/release/rsseguranca-docker"]