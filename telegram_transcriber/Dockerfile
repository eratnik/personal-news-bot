FROM python:3.13-alpine
RUN apk add --no-cache ffmpeg ca-certificates
WORKDIR /app
COPY bot.py /app/bot.py
CMD ["python", "-u", "/app/bot.py"]
