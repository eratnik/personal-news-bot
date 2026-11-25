FROM python:3.13-alpine
RUN pip install --no-cache-dir PyYAML==6.0.3
WORKDIR /app
COPY monitor.py .
ENV HA_INGRESS=1 BIND=0.0.0.0 PYTHONUNBUFFERED=1
CMD ["python", "monitor.py"]
