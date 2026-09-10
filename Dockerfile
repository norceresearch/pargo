# Test image for the integration suite: pargo and nothing else, since every example
# task lives in pargo.utils.
FROM python:3.12-slim

COPY . /src
RUN pip install --no-cache-dir /src && rm -rf /src
