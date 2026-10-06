FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
# Необязательный CA для сборки за корпоративным прокси.
RUN --mount=type=secret,id=build_ca \
    if [ -f /run/secrets/build_ca ]; then export PIP_CERT=/run/secrets/build_ca; fi; \
    pip install --no-cache-dir -r requirements.txt && useradd --uid 10001 --create-home app
COPY common common
COPY fraud_detector fraud_detector
COPY result_writer result_writer
COPY interface interface
COPY scripts scripts
COPY examples examples
USER app
CMD ["python", "-m", "fraud_detector.app.app"]
