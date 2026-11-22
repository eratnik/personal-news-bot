# Telegram Transcriber

1. Install and start the official Whisper app. Select small-int8 and ru.
2. Install Telegram Transcriber from this repository.
3. Enter bot_token in Configuration. It stays in Home Assistant app configuration.
4. Enter allowed_user_ids as a list of numeric Telegram user IDs. An empty list disables startup.
5. Keep whisper_host core-whisper and port 10300.
6. Start the app and forward a voice message in a private chat with your bot.

Dedicated bot required: do not connect the same bot to another polling consumer or webhook.
No external ports or Home Assistant API permissions are required.
Audio is downloaded from Telegram, converted by FFmpeg and transcribed by your local Whisper.
Temporary audio is deleted after processing. Transcripts are sent to Telegram.
The default maximum duration is 300 seconds; download limit is 20 MB.
Long transcripts are sent in several replies. Errors are reported in Telegram and sanitized logs.

Bot credentials are included in app backups: protect your Home Assistant backups.
Requests run sequentially. A container restart during processing can cause the current message to be processed again.
If Telegram delivery fails, forward the recording again after connectivity recovers.
Models and Whisper are managed independently of this app.
