# SPDX-FileCopyrightText: 2026 SEANERGYS Partners <https://seanergys.eu>
# SPDX-License-Identifier: Apache-2.0

FROM flux-demo

ENV DEBIAN_FRONTEND=noninteractive

RUN sudo apt-get update && sudo apt-get install -y --no-install-recommends \
    cmake \
    ninja-build \
    build-essential \
    pkg-config \
    libpq-dev \
    postgresql-client \
    libjansson-dev \
    capnproto \
    libcapnp-dev \
    ripgrep && \
    sudo rm -rf /var/lib/apt/lists/*

ENV LD_LIBRARY_PATH=/opt/seanerbus-v2/lib:/opt/flux/lib:/usr/local/lib

ENV PATH=/opt/seanerbus-v2/bin:/opt/flux/bin:/opt/seanerbus/bin:$PATH

ENV PKG_CONFIG_PATH=/opt/seanerbus-v2/lib/pkgconfig:/opt/flux/lib/pkgconfig:/opt/seanerbus/lib/pkgconfig

WORKDIR /workspace/seanerbus-jobs-database

COPY . .

RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build --parallel
