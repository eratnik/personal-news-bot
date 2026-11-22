"""Private Telegram transcription bridge. No third-party Python dependencies."""
import json
import logging
import os
import re
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

LOG = logging.getLogger("transcriber")
MAX_BYTES = 20_000_000
OPTIONS = Path("/data/options.json")
OFFSET_FILE = Path("/data/offset")
CONFIG = {}
API = ""


def telegram(method, data=None):
    request = urllib.request.Request(
        API + method,
        data=json.dumps(data or {}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=65) as response:
        result = json.load(response)
    if not result.get("ok"):
        raise RuntimeError("Telegram rejected the request")
    return result["result"]


def reply(message, text):
    # Telegram counts UTF-16 code units. 1800 code points fit even with emoji.
    for start in range(0, len(text), 1800):
        telegram("sendMessage", {
            "chat_id": message["chat"]["id"],
            "text": text[start:start + 1800],
            "reply_parameters": {
                "message_id": message["message_id"],
                "allow_sending_without_reply": True,
            },
        })


def send_event(stream, event_type, data=None, payload=b""):
    event = {"type": event_type}
    if data is not None:
        event["data"] = data
    if payload:
        event["payload_length"] = len(payload)
    stream.write(json.dumps(event).encode() + b"\n")
    if payload:
        stream.write(payload)
    stream.flush()


def read_exact(stream, length):
    chunks = bytearray()
    while len(chunks) < length:
        chunk = stream.read(length - len(chunks))
        if not chunk:
            raise EOFError("Whisper disconnected")
        chunks.extend(chunk)
    return bytes(chunks)


def transcribe(pcm_path):
    timeout = CONFIG["transcription_timeout"]
    deadline = time.monotonic() + timeout
    with socket.create_connection(
        (CONFIG["whisper_host"], CONFIG["whisper_port"]), timeout=15
    ) as connection:
        with connection.makefile("rwb") as stream:
            send_event(stream, "transcribe", {"language": CONFIG["language"]})
            audio_format = {"rate": 16000, "width": 2, "channels": 1}
            send_event(stream, "audio-start", audio_format)
            with open(pcm_path, "rb") as audio:
                while chunk := audio.read(32000):
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Transcription deadline exceeded")
                    send_event(stream, "audio-chunk", audio_format, chunk)
            send_event(stream, "audio-stop")
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Transcription deadline exceeded")
                connection.settimeout(remaining)
                line = stream.readline(65537)
                if not line or len(line) > 65536:
                    raise RuntimeError("Invalid Whisper response")
                event = json.loads(line)
                data = event.get("data") or {}
                data_length = int(event.get("data_length", 0))
                payload_length = int(event.get("payload_length", 0))
                if not 0 <= data_length <= 1_000_000 or not 0 <= payload_length <= 1_000_000:
                    raise RuntimeError("Whisper response too large")
                if data_length:
                    data.update(json.loads(read_exact(stream, data_length)))
                if payload_length:
                    read_exact(stream, payload_length)
                if event["type"] == "transcript":
                    return str(data.get("text", "")).strip()
                if event["type"] == "error":
                    raise RuntimeError("Whisper returned an error")


def handle(message):
    if message.get("from", {}).get("id") not in CONFIG["allowed_user_ids"]:
        LOG.warning("Message ignored: sender ID %s is absent from allowed_user_ids", message.get("from", {}).get("id"))
        return
    if message.get("chat", {}).get("type") != "private":
        LOG.warning("Message ignored: use a private chat with the bot")
        return
    LOG.info("Authorized message received; checking audio")
    audio = message.get("voice") or message.get("audio")
    if not audio:
        reply(message, "Перешлите голосовое сообщение или аудио до "
              + str(CONFIG["max_duration_seconds"]) + " секунд.")
        return
    if audio.get("duration", 0) > CONFIG["max_duration_seconds"]:
        reply(message, "Запись превышает установленный лимит длительности.")
        return
    if audio.get("file_size", 0) > MAX_BYTES:
        reply(message, "Файл превышает лимит 20 МБ.")
        return
    reply(message, "Распознаю…")
    with tempfile.TemporaryDirectory(prefix="telegram-audio-") as directory:
        source = Path(directory) / "input"
        pcm = Path(directory) / "audio.pcm"
        remote = telegram("getFile", {"file_id": audio["file_id"]})
        if remote.get("file_size", 0) > MAX_BYTES:
            raise ValueError("Audio file too large")
        url = "https://api.telegram.org/file/bot" + CONFIG["bot_token"] + "/" + remote["file_path"]
        with urllib.request.urlopen(url, timeout=60) as response, source.open("wb") as output:
            total = 0
            while chunk := response.read(65536):
                total += len(chunk)
                if total > MAX_BYTES:
                    raise ValueError("Audio file too large")
                output.write(chunk)
        conversion = subprocess.run([
            "ffmpeg", "-nostdin", "-v", "error", "-i", str(source),
            "-t", str(CONFIG["max_duration_seconds"]), "-vn",
            "-ar", "16000", "-ac", "1", "-f", "s16le", str(pcm),
        ], capture_output=True, timeout=60, check=False)
        if conversion.returncode:
            raise RuntimeError("Audio conversion failed")
        text = transcribe(pcm)
        reply(message, text or "Речь не распознана. Попробуйте запись без фонового шума.")


def main():
    global CONFIG, API
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    CONFIG = json.loads(OPTIONS.read_text())
    if not CONFIG.get("bot_token") or not CONFIG.get("allowed_user_ids"):
        raise SystemExit("Заполните bot_token и allowed_user_ids в настройках приложения.")
    token = str(CONFIG["bot_token"]).strip()
    if not re.fullmatch(r"[0-9]+:[A-Za-z0-9_-]+", token):
        raise SystemExit("Некорректный bot_token: вставьте только токен BotFather без кавычек, URL, пробелов и переносов.")
    CONFIG["bot_token"] = token
    API = "https://api.telegram.org/bot" + token + "/"
    # Polling belongs exclusively to this dedicated bot.
    offset = int(OFFSET_FILE.read_text()) if OFFSET_FILE.exists() else 0
    LOG.info("Telegram Transcriber started; audio and tokens are not logged.")
    identity_logged = False
    while True:
        try:
            if not identity_logged:
                identity = telegram("getMe")
                LOG.info("Connected bot: @%s", identity.get("username", "unknown"))
                identity_logged = True
            updates = telegram("getUpdates", {
                "offset": offset, "timeout": 30, "limit": 10,
                "allowed_updates": ["message"],
            })
            for update in updates:
                message = update.get("message")
                if message:
                    try:
                        handle(message)
                    except Exception as error:
                        LOG.warning("Message processing failed: %s", type(error).__name__)
                        try:
                            if message.get("from", {}).get("id") in CONFIG["allowed_user_ids"]:
                                reply(message, "Не удалось распознать запись. Проверьте, запущен ли Whisper, и повторите отправку.")
                        except Exception:
                            LOG.warning("Could not send failure notice")
                offset = update["update_id"] + 1
                temporary = OFFSET_FILE.with_suffix(".tmp")
                temporary.write_text(str(offset))
                os.replace(temporary, OFFSET_FILE)
        except urllib.error.HTTPError as error:
            LOG.warning("Telegram HTTP %s. For 409 ensure this bot has no other polling client or webhook.", error.code)
            time.sleep(10)
        except Exception as error:
            LOG.warning("Polling failed: %s", type(error).__name__)
            time.sleep(10)


if __name__ == "__main__":
    main()
