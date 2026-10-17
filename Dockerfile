FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && useradd --create-home --uid 10001 collector \
    && mkdir -p /app/data /app/logs \
    && chown collector:collector /app/data /app/logs
COPY --chown=collector:collector fetch.py .
COPY --chown=collector:collector pipeline pipeline
COPY --chown=collector:collector datahub datahub
USER collector
ENTRYPOINT ["python", "fetch.py"]
CMD ["--run"]
