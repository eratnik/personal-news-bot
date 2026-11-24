FROM python:3.13-slim
WORKDIR /app
COPY app.py catalog_match.py registry_export.py globalsign-r6-alphassl-2025.pem ./
CMD ["python", "-u", "app.py"]
