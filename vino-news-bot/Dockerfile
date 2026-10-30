ARG BUILD_FROM
FROM $BUILD_FROM

WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py /app/
CMD ["python", "/app/app.py"]
