import asyncio
import html
import json
import logging
import re
from pathlib import Path
from urllib.parse import urljoin, urlparse

import feedparser
import httpx
from bs4 import BeautifulSoup

OPTIONS_FILE = Path("/data/options.json")
STATE_FILE = Path("/data/state.json")
MAX_SEEN = 2000
LOG = logging.getLogger("vino-news-bot")


def load_json(path, fallback):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return fallback


def save_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")


def clean_text(value):
    return re.sub(r"\s+", " ", value or "").strip()


def is_same_site(url, source):
    return urlparse(url).netloc.lower().removeprefix("www.") == urlparse(source).netloc.lower().removeprefix("www.")


def feed_candidates(source):
    root = source.rstrip("/")
    return [source, f"{root}/feed/", f"{root}/rss/", f"{root}/atom/"]


def parse_feed(body):
    parsed = feedparser.parse(body)
    if not parsed.entries:
        return []
    items = []
    for entry in parsed.entries[:50]:
        link = entry.get("link")
        title = clean_text(entry.get("title", ""))
        if link and title:
            items.append({"url": link, "title": title, "summary": clean_text(entry.get("summary", ""))})
    return items


def parse_html(body, source):
    soup = BeautifulSoup(body, "html.parser")

    # VinoItaliano is a static site: the title is a .grey-background
    # sibling and the following non-button link is the article itself.
    vino_main = soup.select_one("#mainnew")
    if vino_main:
        vino_items = []
        for heading in vino_main.select(".grey-background"):
            article = heading.find_next("a", href=True)
            while article and article.get("id") == "button1":
                article = article.find_next("a", href=True)
            if not article:
                continue
            title = clean_text(heading.get_text(" ", strip=True))
            href = urljoin(source, article["href"])
            if title and href and is_same_site(href, source):
                vino_items.append({
                    "url": href,
                    "title": title,
                    "summary": clean_text(article.get_text(" ", strip=True))[:700],
                })
        if vino_items:
            return vino_items[:50]

    selectors = ["article h1 a", "article h2 a", "article h3 a", ".entry-title a", ".post-title a", "main h2 a", "main h3 a"]
    anchors = []
    for selector in selectors:
        anchors.extend(soup.select(selector))
    if not anchors:
        anchors = soup.select("article a[href]")

    found, seen = [], set()
    for anchor in anchors:
        href = urljoin(source, anchor.get("href", ""))
        title = clean_text(anchor.get_text(" ", strip=True))
        if not href or not title or href in seen or not is_same_site(href, source):
            continue
        if len(title) < 8 or href.rstrip("/") == source.rstrip("/"):
            continue
        seen.add(href)
        found.append({"url": href, "title": title, "summary": ""})
    return found[:50]


def vk_group_id(value):
    """Extract a numeric ID from a VK club/public URL or identifier."""
    value = str(value).strip()
    match = re.search(r"(?:club|public)(\d+)", value, re.IGNORECASE)
    if match:
        return int(match.group(1))
    if value.isdigit():
        return int(value)
    return None


async def resolve_vk_group_id(client, value, access_token):
    """Resolve a short VK community address, such as vk.com/mastersland."""
    group_id = vk_group_id(value)
    if group_id:
        return group_id

    raw = str(value).strip()
    parsed = urlparse(raw if "://" in raw else f"https://vk.com/{raw}")
    screen_name = parsed.path.strip("/").split("/")[0]
    if not screen_name:
        return None
    try:
        response = await client.get(
            "https://api.vk.com/method/utils.resolveScreenName",
            params={"screen_name": screen_name, "access_token": access_token, "v": "5.199"},
        )
        response.raise_for_status()
        result = response.json().get("response", {})
        if result.get("object_type") in {"group", "page"} and result.get("object_id"):
            return int(result["object_id"])
    except (httpx.HTTPError, ValueError) as error:
        LOG.warning("VK community %s: could not resolve address: %s", value, error)
    return None


async def fetch_vk_items(client, group_id, access_token):
    """Read recent original posts from a public VK community."""
    try:
        response = await client.get(
            "https://api.vk.com/method/wall.get",
            params={
                "owner_id": f"-{group_id}",
                "count": 20,
                "filter": "owner",
                "access_token": access_token,
                "v": "5.199",
            },
        )
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as error:
        LOG.warning("VK group %s: request failed: %s", group_id, error)
        return []

    if payload.get("error"):
        LOG.warning("VK group %s: API error: %s", group_id, payload["error"].get("error_msg", "unknown error"))
        return []

    items = []
    for post in payload.get("response", {}).get("items", []):
        if post.get("is_pinned") or not post.get("id"):
            continue
        text = clean_text(post.get("text", ""))
        title = text[:120] if text else "Новая запись VK"
        items.append({
            "url": f"https://vk.com/wall-{group_id}_{post['id']}",
            "title": title,
            "summary": text[:700],
        })
    return items


async def fetch_items(client, source):
    for candidate in feed_candidates(source):
        try:
            response = await client.get(candidate)
            response.raise_for_status()
            items = parse_feed(response.content)
            if items:
                LOG.info("%s: RSS found at %s", source, candidate)
                return items
        except (httpx.HTTPError, ValueError) as error:
            LOG.debug("%s unavailable: %s", candidate, error)

    try:
        response = await client.get(source)
        response.raise_for_status()
        items = parse_html(response.text, source)
        if items:
            LOG.info("%s: using article-page extraction", source)
        return items
    except httpx.HTTPError as error:
        https_error = error

    # Some legacy public sites still offer plain HTTP but cannot negotiate
    # current TLS. Do not follow redirects here: an HTTPS redirect would
    # merely repeat the failed handshake.
    if source.startswith("https://"):
        fallback = "http://" + source.removeprefix("https://")
        try:
            response = await client.get(fallback, follow_redirects=False)
            response.raise_for_status()
            items = parse_feed(response.content) or parse_html(response.text, fallback)
            if items:
                LOG.info("%s: using HTTP fallback for legacy TLS", source)
                return items
        except (httpx.HTTPError, ValueError) as http_error:
            LOG.warning("%s: cannot retrieve source over HTTPS (%s) or HTTP (%s)", source, https_error, http_error)
            return []

    LOG.warning("%s: cannot retrieve source: %s", source, https_error)
    return []


async def send_message(client, token, chat_id, item, source):
    title = html.escape(item["title"])
    summary = html.escape(item.get("summary", ""))[:700]
    host = html.escape(urlparse(source).netloc)
    text = f"<b>{host}</b>\n<a href=\"{item['url']}\">{title}</a>"
    if summary:
        text += f"\n\n{summary}"
    response = await client.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={"chat_id": chat_id, "text": text[:4000], "parse_mode": "HTML", "disable_web_page_preview": False},
    )
    response.raise_for_status()


async def discover_chats(client, token):
    try:
        response = await client.get(f"https://api.telegram.org/bot{token}/getUpdates", params={"timeout": 0})
        response.raise_for_status()
        for update in response.json().get("result", []):
            message = update.get("message") or update.get("my_chat_member")
            chat = message.get("chat", {}) if isinstance(message, dict) else {}
            if chat.get("id"):
                LOG.info("Telegram chat discovered: %s (%s)", chat["id"], chat.get("type", "unknown"))
    except (httpx.HTTPError, ValueError) as error:
        LOG.warning("Telegram connection failed: %s", error)


async def check_once(options, state, client):
    token = options.get("telegram_token", "").strip()
    recipients = [str(value).strip() for value in options.get("recipient_chat_ids", []) if str(value).strip()]
    sources = [value.strip() for value in options.get("source_urls", []) if value.strip()]
    if not token:
        LOG.error("Set telegram_token in the add-on configuration before starting.")
        return state

    if not recipients:
        await discover_chats(client, token)
        LOG.warning("No recipient_chat_ids configured. Send /start to the bot, read this log for the chat ID, then add it to configuration.")
        return state

    for source in sources:
        items = await fetch_items(client, source)
        if not items:
            continue
        key = source.rstrip("/")
        known = state.setdefault(key, [])
        known_set = set(known)
        fresh = [item for item in items if item["url"] not in known_set]
        if not known and not options.get("send_existing_on_first_run", False):
            LOG.info("%s: first scan complete; %s existing posts saved without delivery", source, len(items))
            fresh = []
        for item in reversed(fresh):
            delivered = True
            for chat_id in recipients:
                try:
                    await send_message(client, token, chat_id, item, source)
                except httpx.HTTPError as error:
                    LOG.error("Could not send '%s' to %s: %s", item["title"], chat_id, error)
                    delivered = False
                    break
            if delivered:
                known_set.add(item["url"])
        state[key] = list(known_set)[:MAX_SEEN]

    vk_token = options.get("vk_service_token", "").strip()
    vk_sources = [
        *options.get("vk_group_ids", []),  # legacy numeric format
        *options.get("vk_group_urls", []),
    ]
    processed_vk_groups = set()
    for value in vk_sources if vk_token else []:
        group_id = await resolve_vk_group_id(client, value, vk_token)
        if not group_id:
            LOG.warning("Ignoring invalid or inaccessible VK community: %s", value)
            continue
        if group_id in processed_vk_groups:
            continue
        processed_vk_groups.add(group_id)
        items = await fetch_vk_items(client, group_id, vk_token)
        if not items:
            continue
        key = f"vk:{group_id}"
        source = f"https://vk.com/club{group_id}"
        known = state.setdefault(key, [])
        known_set = set(known)
        fresh = [item for item in items if item["url"] not in known_set]
        if not known and not options.get("send_existing_on_first_run", False):
            LOG.info("VK group %s: first scan complete; %s existing posts saved without delivery", group_id, len(items))
            fresh = []
        for item in reversed(fresh):
            delivered = True
            for chat_id in recipients:
                try:
                    await send_message(client, token, chat_id, item, source)
                except httpx.HTTPError as error:
                    LOG.error("Could not send VK post to %s: %s", chat_id, error)
                    delivered = False
                    break
            if delivered:
                known_set.add(item["url"])
        state[key] = list(known_set)[:MAX_SEEN]
    return state


async def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    options = load_json(OPTIONS_FILE, {})
    state = load_json(STATE_FILE, {})
    interval = max(5, min(1440, int(options.get("check_interval_minutes", 60))))
    timeout = httpx.Timeout(25.0, connect=15.0)
    headers = {"User-Agent": "PersonalNewsBot/0.1 (+private RSS-to-Telegram notifier)"}
    LOG.info("Starting; checking %s source(s) every %s minute(s)", len(options.get("source_urls", [])), interval)
    async with httpx.AsyncClient(timeout=timeout, headers=headers, follow_redirects=True) as client:
        while True:
            options = load_json(OPTIONS_FILE, {})
            state = await check_once(options, state, client)
            save_json(STATE_FILE, state)
            await asyncio.sleep(max(5, min(1440, int(options.get("check_interval_minutes", interval)))) * 60)


if __name__ == "__main__":
    asyncio.run(main())
