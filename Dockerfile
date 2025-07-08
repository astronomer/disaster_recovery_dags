FROM quay.io/astronomer/astro-runtime:12.9.0
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends skopeo \
 && rm -rf /var/lib/apt/lists/*
USER astro