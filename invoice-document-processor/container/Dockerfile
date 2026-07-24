FROM node:22-bookworm-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    qpdf \
    poppler-utils \
    imagemagick \
    img2pdf \
    tini \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY server.mjs /app/server.mjs
COPY policy.xml /etc/ImageMagick-6/policy.xml

ENV PORT=8080
EXPOSE 8080
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["node", "/app/server.mjs"]
