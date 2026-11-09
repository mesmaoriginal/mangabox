#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MangaBox → کاور + اطلاعات سایت + چپتر به چپتر (PDF) → کانال تلگرام
نسخه ۲: پایدارتر، سریع‌تر، با پشتیبانی از ادامه کار

نصب:
    pip install requests beautifulsoup4 pillow img2pdf "python-telegram-bot>=21"

اجرا:
    export BOT_TOKEN="123456:ABC..."
    export CHANNEL="@YourChannel"
    python mangabox_to_telegram.py --slug i-shall-master-this-family
    python mangabox_to_telegram.py --slug my-slug --chapters 1 2 3
    python mangabox_to_telegram.py --all
    python mangabox_to_telegram.py --slug my-slug --no-upload      # فقط ساخت PDF
"""

import argparse
import asyncio
import html
import io
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import img2pdf
import requests
from bs4 import BeautifulSoup
from PIL import Image
from requests.adapters import HTTPAdapter
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import NetworkError, RetryAfter, TimedOut
from telegram.request import HTTPXRequest
from urllib3.util.retry import Retry

# ======================== تنظیمات ========================
CDN_BASE = "https://cdn.mangabox.ir/chapters"
SITE_BASE = "https://mangabox.ir"
OUTPUT_DIR = Path("manhwa_archive")
CACHE_FILE = Path("manhwa_map.json")
PROGRESS_FILE = Path("progress.json")

TIMEOUT = 40
MAX_RETRIES = 5
DELAY = 1.0                    # مکث بین چپترها (ثانیه)
JPEG_QUALITY = 72
MAX_IMAGE_WIDTH = 1100

# محدودیت آپلود ربات تلگرام ۵۰MB است؛ با حاشیه امن تقسیم می‌کنیم
PART_MAX_BYTES = 45 * 1024 * 1024

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://mangabox.ir/",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "fa-IR,fa;q=0.9,en;q=0.8",
}

IMAGE_EXTS = (".webp", ".jpg", ".jpeg", ".png")

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("mbox")
for _n in ("httpx", "httpcore", "telegram"):
    logging.getLogger(_n).setLevel(logging.WARNING)   # جلوگیری از چاپ توکن در لاگ


class ChapterError(Exception):
    """خطای قابل‌گزارش برای یک چپتر."""


# ======================== شبکه ========================
def make_session(workers: int) -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=workers * 2, pool_maxsize=workers * 2)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


SESSION = make_session(8)


def get_bytes(url: str) -> bytes | None:
    try:
        r = SESSION.get(url, timeout=TIMEOUT)
        r.raise_for_status()
        return r.content
    except requests.RequestException as e:
        log.debug("download failed %s: %s", url, e)
        return None


def get_html(url: str) -> str | None:
    data = get_bytes(url)
    return data.decode("utf-8", errors="replace") if data else None


# ======================== ابزارها ========================
def natural_key(s):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", str(s))]


def extract_page_num(name: str) -> int:
    m = re.search(r"page_(\d+)_", name, re.I) or re.search(r"(\d+)", name)
    return int(m.group(1)) if m else 99999


def get_listing(url: str) -> list[str]:
    page = get_html(url)
    if not page:
        return []
    soup = BeautifulSoup(page, "html.parser")
    names = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        name = a.get_text(strip=True)
        if not href or href.startswith("?") or href in ("../", "/"):
            continue
        if name in ("Parent Directory", "Name", "Last Modified", "Size"):
            continue
        clean = href.rstrip("/").split("/")[-1]
        if clean:
            names.append(clean)
    return names


def safe_name(s: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", s).strip().replace(" ", "_")


def make_hashtag(text: str, limit: int = 28) -> str:
    t = re.sub(r"\s+", "_", text.strip())
    t = re.sub(r"[^\w]", "", t)[:limit]
    return f"#{t}" if t else ""


# ======================== پیشرفت (ذخیره اتمیک) ========================
def load_json(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            log.warning("⚠ فایل %s خراب بود، از نو ساخته می‌شود", path.name)
    return {}


def save_json(path: Path, data: dict):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


# ---------- Supabase (اختیاری): اگر SUPABASE_URL و SUPABASE_KEY تنظیم باشد، پیشرفت آنجا ذخیره می‌شود ----------
SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip().strip("\"'").rstrip("/")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip().strip("\"'")
USE_DB = bool(SUPABASE_URL and SUPABASE_KEY)
DB_TABLE = "tg_progress"
DEADLINE: float | None = None     # زمان توقف نرم (--max-minutes)


def _db_headers(extra: dict | None = None) -> dict:
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if extra:
        h.update(extra)
    return h


def db_fetch_all() -> list[dict]:
    rows, offset = [], 0
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{DB_TABLE}",
            params={"select": "slug,chapter,status,data", "limit": 1000, "offset": offset,
                    "order": "slug,chapter"},
            headers=_db_headers(), timeout=30,
        )
        r.raise_for_status()
        batch = r.json()
        rows += batch
        if len(batch) < 1000:
            return rows
        offset += 1000


def db_upsert_many(rows: list[dict]):
    """همه‌ی ردیف‌های یک فراخوانی باید کلیدهای یکسان داشته باشند."""
    for i in range(0, len(rows), 500):
        chunk = rows[i:i + 500]
        for attempt in range(3):
            try:
                r = requests.post(
                    f"{SUPABASE_URL}/rest/v1/{DB_TABLE}",
                    params={"on_conflict": "slug,chapter"},
                    headers=_db_headers({"Content-Type": "application/json",
                                         "Prefer": "resolution=merge-duplicates,return=minimal"}),
                    data=json.dumps(chunk).encode("utf-8"), timeout=30,
                )
                r.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt == 2:
                    log.warning("⚠ ذخیره در Supabase ناموفق: %s", e)
                else:
                    time.sleep(2)


def load_state() -> tuple[dict, dict]:
    """(progress, series_map) را از Supabase یا فایل‌های محلی می‌خواند."""
    if not USE_DB:
        return load_json(PROGRESS_FILE), load_json(CACHE_FILE)
    progress, mapping = {}, {}
    for row in db_fetch_all():
        if row["chapter"] == "_info":
            if row.get("data"):
                mapping[row["slug"]] = row["data"]
        else:
            progress.setdefault(row["slug"], {})[row["chapter"]] = row["status"]
    log.info("☁ از Supabase خوانده شد: %d مانهوا در پیشرفت، %d در کش", len(progress), len(mapping))
    return progress, mapping


def save_info(mapping: dict, slug: str):
    if USE_DB:
        db_upsert_many([{"slug": slug, "chapter": "_info", "status": "info", "data": mapping[slug]}])
    else:
        save_json(CACHE_FILE, mapping)


def is_done(progress, slug, chapter) -> bool:
    return progress.get(slug, {}).get(str(chapter)) == "done"


def mark(progress, slug, chapter, state: str):
    progress.setdefault(slug, {})[str(chapter)] = state
    if USE_DB:
        db_upsert_many([{"slug": slug, "chapter": str(chapter), "status": state}])
    else:
        save_json(PROGRESS_FILE, progress)


def mark_many(progress, slug, chapters: list[str], state: str):
    for ch in chapters:
        progress.setdefault(slug, {})[str(ch)] = state
    if USE_DB:
        db_upsert_many([{"slug": slug, "chapter": str(ch), "status": state} for ch in chapters])
    else:
        save_json(PROGRESS_FILE, progress)


# ======================== اطلاعات سایت ========================
KNOWN_GENRES = {
    "اکشن", "فانتزی", "عاشقانه", "درام", "کمدی", "دخترانه", "تناسخی", "ترسناک",
    "روزمره", "علمی تخیلی", "ماجراجویی", "هنرهای رزمی", "روانشناختی", "معمایی",
    "ورزشی", "تناسخ",
}


def discover_series(slug: str) -> dict | None:
    log.info("  🔍 گرفتن اطلاعات: %s", slug)
    page = get_html(f"{SITE_BASE}/work/{slug}")
    if not page:
        return None
    soup = BeautifulSoup(page, "html.parser")

    h1 = soup.find("h1")
    fa = h1.get_text(strip=True) if h1 else ""

    en = ""
    title_tag = soup.find("title")
    if title_tag:
        m = re.search(r"\(([^)]+)\)\s*\|\s*مانگا باکس", title_tag.get_text())
        if m:
            en = m.group(1).strip()
    en = en or slug.replace("-", " ").title()
    fa = fa or en

    rating = ""
    for el in soup.find_all(string=re.compile(r"^[۰-۹0-9]+\.[۰-۹0-9]+$")):
        val = el.strip()
        if 3 <= len(val) <= 5:
            rating = val
            break
    if not rating:
        m = re.search(r"([۰-۹0-9]{1,2}\.[۰-۹0-9])\s*از", page)
        if m:
            rating = m.group(1)

    text = soup.get_text(" ", strip=True)
    status = "پایان یافته" if "پایان یافته" in text else ("در حال انتشار" if "در حال انتشار" in text else "")

    genres = []
    for a in soup.find_all("a", href=True):
        if "genre" in a["href"]:
            g = a.get_text(strip=True)
            if g in KNOWN_GENRES and g not in genres:
                genres.append(g)
    genres = genres[:5]

    m = re.search(r"سال[:\s]*([۱۲۳۴۵۶۷۸۹۰0-9]{4})", page)
    year = m.group(1) if m else ""

    cover_url = None
    og = soup.find("meta", property="og:image")
    if og and og.get("content"):
        cover_url = og["content"]
    if not cover_url:
        for img in soup.find_all("img"):
            src = img.get("src") or img.get("data-src") or ""
            if not src or any(x in src.lower() for x in ("logo", "icon", "avatar", "emoji", "svg", "enamad")):
                continue
            cover_url = src
            break
    if cover_url:
        if cover_url.startswith("//"):
            cover_url = "https:" + cover_url
        elif cover_url.startswith("/"):
            cover_url = SITE_BASE + cover_url

    cdn_id = None
    for ch in ("1", "2", "3"):
        ch_html = get_html(f"{SITE_BASE}/read/{slug}/{ch}")
        if not ch_html:
            continue
        m = re.search(r"cdn\.mangabox\.ir/chapters/(\d+)/", ch_html) or \
            re.search(r"chapters[/\\]+(\d+)[/\\]+", ch_html)
        if m:
            cdn_id = m.group(1)
            break
    if not cdn_id:
        log.warning("  ⚠ ID پیدا نشد")
        return None

    info = {
        "slug": slug, "id": cdn_id, "en": en, "fa": fa, "rating": rating,
        "status": status, "genres": genres, "year": year, "cover_url": cover_url,
    }
    log.info("  ✓ %s | %s", en, fa)
    log.info("    امتیاز: %s | وضعیت: %s | ژانر: %s",
             rating or "—", status or "—", ", ".join(genres) or "—")
    return info


def load_map(slugs: list[str], mapping: dict, refresh: bool = False) -> dict:
    for slug in slugs:
        if slug in mapping and not refresh:
            log.info("  ↺ از کش: %s", slug)
            continue
        info = discover_series(slug)
        if info:
            mapping[slug] = info
            save_info(mapping, slug)
        time.sleep(1.0)
    return mapping


def get_all_slugs() -> list[str]:
    page = get_html(f"{SITE_BASE}/archive")
    if not page:
        return []
    soup = BeautifulSoup(page, "html.parser")
    slugs = set()
    for a in soup.find_all("a", href=True):
        m = re.search(r"/work/([a-z0-9\-]+)", a["href"])
        if m:
            slugs.add(m.group(1))
    return sorted(slugs)


# ======================== کاور ========================
def download_cover(info: dict, out_dir: Path) -> Path | None:
    if not info.get("cover_url"):
        return None
    data = get_bytes(info["cover_url"])
    if not data:
        return None
    path = out_dir / f"cover_{info['id']}.jpg"
    try:
        with Image.open(io.BytesIO(data)) as im:
            im.convert("RGB").save(path, "JPEG", quality=90)
    except Exception as e:  # noqa: BLE001
        log.warning("  ⚠ ذخیره کاور: %s", e)
        path.write_bytes(data)
    return path


# ======================== ساخت PDF ========================
def process_image(data: bytes, width_limit: int, quality: int) -> bytes | None:
    """تبدیل به JPEG کم‌حجم (داخل حافظه، بدون فایل موقت)."""
    try:
        with Image.open(io.BytesIO(data)) as im:
            im.load()
            if im.mode in ("RGBA", "LA") or (im.mode == "P" and "transparency" in im.info):
                rgba = im.convert("RGBA")
                bg = Image.new("RGB", rgba.size, (255, 255, 255))
                bg.paste(rgba, mask=rgba.split()[-1])
                im = bg
            elif im.mode != "RGB":
                im = im.convert("RGB")

            if im.width > width_limit:
                ratio = width_limit / im.width
                im = im.resize((width_limit, max(1, int(im.height * ratio))), Image.Resampling.LANCZOS)

            buf = io.BytesIO()
            im.save(buf, "JPEG", quality=quality, optimize=True)
            return buf.getvalue()
    except Exception:  # noqa: BLE001
        return None


def split_by_size(pages: list[bytes], limit: int) -> list[list[bytes]]:
    parts, cur, size = [], [], 0
    for p in pages:
        if cur and size + len(p) > limit:
            parts.append(cur)
            cur, size = [], 0
        cur.append(p)
        size += len(p)
    if cur:
        parts.append(cur)
    return parts


def build_chapter_pdfs(info: dict, chapter: str, out_dir: Path, workers: int,
                       quality: int, width_limit: int) -> list[Path]:
    """PDF چپتر را می‌سازد؛ اگر از حد تلگرام بزرگ‌تر شد چند پارت می‌کند."""
    chapter_url = f"{CDN_BASE}/{info['id']}/{chapter}/"
    files = [f for f in get_listing(chapter_url) if f.lower().endswith(IMAGE_EXTS)]
    files.sort(key=lambda f: (extract_page_num(f), f))
    if not files:
        raise ChapterError("صفحه‌ای پیدا نشد")

    def work(fname: str) -> bytes | None:
        raw = get_bytes(f"{chapter_url}{fname}")
        return process_image(raw, width_limit, quality) if raw else None

    with ThreadPoolExecutor(max_workers=workers) as ex:
        results = list(ex.map(work, files))   # ترتیب حفظ می‌شود

    missing = [f for f, r in zip(files, results) if r is None]
    if missing:
        raise ChapterError(f"{len(missing)} از {len(files)} صفحه دانلود/تبدیل نشد")

    base = f"{safe_name(info['en'])}_Ch{str(chapter).zfill(3)}"
    parts = split_by_size(results, PART_MAX_BYTES)
    paths = []
    for i, part in enumerate(parts, 1):
        name = f"{base}.pdf" if len(parts) == 1 else f"{base}_part{i}.pdf"
        path = out_dir / name
        tmp = path.with_suffix(".pdf.part")
        tmp.write_bytes(img2pdf.convert(part))
        os.replace(tmp, path)
        paths.append(path)
    return paths


# ======================== تلگرام ========================
async def with_retry(factory, attempts: int = 4):
    last = None
    for i in range(1, attempts + 1):
        try:
            return await factory()
        except RetryAfter as e:
            wait = e.retry_after
            wait = wait.total_seconds() if hasattr(wait, "total_seconds") else wait
            log.info("    ⏳ محدودیت تلگرام، %s ثانیه صبر...", wait)
            await asyncio.sleep(float(wait) + 1)
            last = e
        except (TimedOut, NetworkError) as e:
            await asyncio.sleep(3 * i)
            last = e
    raise last


def make_intro_caption(info: dict, total_chapters: int) -> str:
    esc = html.escape
    lines = [f"📖 <b>{esc(info['en'])}</b>", f"🇮🇷 {esc(info['fa'])}", ""]
    if info.get("status"):
        lines.append(f"▫️ {esc(info['status'])}")
    if info.get("rating") and len(str(info["rating"])) <= 5:
        lines.append(f"⭐ امتیاز: {esc(str(info['rating']))}")
    lines.append(f"📚 {total_chapters} چپتر")

    genres = [g for g in info.get("genres", []) if len(g) < 20][:4]
    if genres:
        lines.append(f"🏷 {' · '.join(esc(g) for g in genres)}")
    lines.append("")

    tags = [make_hashtag(info["en"]), make_hashtag(info["fa"], 22)]
    tags += [make_hashtag(g) for g in genres[:3]]
    tags.append("#مانهوا")
    lines.append(" ".join(t for t in tags if t))
    return "\n".join(lines)[:1024]


async def send_intro(bot: Bot, channel: str, info: dict, total: int, cover: Path | None) -> bool:
    caption = make_intro_caption(info, total)
    try:
        if cover and cover.exists():
            async def send_photo():
                with open(cover, "rb") as f:
                    return await bot.send_photo(chat_id=channel, photo=f, caption=caption,
                                                parse_mode=ParseMode.HTML)
            await with_retry(send_photo)
        else:
            await with_retry(lambda: bot.send_message(chat_id=channel, text=caption,
                                                       parse_mode=ParseMode.HTML))
        log.info("  ✓ معرفی ارسال شد")
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("  ⚠ معرفی ارسال نشد: %s", e)
        return False


async def send_pdf(bot: Bot, channel: str, pdf: Path, info: dict, chapter: str,
                   part: tuple[int, int] | None) -> bool:
    esc = html.escape
    part_txt = f" (بخش {part[0]} از {part[1]})" if part and part[1] > 1 else ""
    caption = (
        f"📖 <b>{esc(info['en'])}</b>\n"
        f"🇮🇷 {esc(info['fa'])}\n\n"
        f"▫️ چپتر <b>{esc(str(chapter))}</b>{part_txt}\n"
        f"#Ch{str(chapter).replace('.', '_')}"
    )

    async def once():
        with open(pdf, "rb") as f:
            return await bot.send_document(chat_id=channel, document=f, filename=pdf.name,
                                           caption=caption, parse_mode=ParseMode.HTML)
    try:
        await with_retry(once)
        return True
    except Exception as e:  # noqa: BLE001
        log.error("    ✗ آپلود: %s", e)
        return False


# ======================== اجرا ========================
async def process_one_manhwa(bot: Bot | None, args, info: dict, progress: dict) -> list[str]:
    slug = info["slug"]
    log.info("\n%s\n  %s  |  %s\n%s", "═" * 60, info["en"], info["fa"], "═" * 60)

    chapters = [c for c in await asyncio.to_thread(get_listing, f"{CDN_BASE}/{info['id']}/")
                if re.fullmatch(r"\d+(\.\d+)?", c)]
    chapters.sort(key=natural_key)
    if args.chapters:
        wanted = {str(c) for c in args.chapters}
        chapters = [c for c in chapters if c in wanted]
    log.info("  تعداد چپتر: %d", len(chapters))

    if args.baseline:
        todo = [c for c in chapters if not is_done(progress, slug, c)]
        mark_many(progress, slug, todo, "done")
        if progress.get(slug, {}).get("_intro") != "sent":
            mark(progress, slug, "_intro", "sent")
        log.info("  ⏭ %d چپتر موجود به‌عنوان انجام‌شده ثبت شد", len(todo))
        return []

    out_dir = OUTPUT_DIR / f"{info['id']}_{slug}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # معرفی فقط یک‌بار (حتی اگر چپتر اول ناموفق بوده باشد)
    if bot and progress.get(slug, {}).get("_intro") != "sent":
        cover = await asyncio.to_thread(download_cover, info, out_dir)
        if await send_intro(bot, args.channel, info, len(chapters), cover):
            mark(progress, slug, "_intro", "sent")
        await asyncio.sleep(1.5)

    failed: list[str] = []
    for ch in chapters:
        if is_done(progress, slug, ch):
            continue
        if DEADLINE and time.time() > DEADLINE:
            log.info("⏱ زمان اجرا تمام شد؛ بقیه در اجرای بعدی ادامه پیدا می‌کند")
            break

        log.info("  → چپتر %s ...", ch)
        try:
            pdfs = await asyncio.to_thread(
                build_chapter_pdfs, info, ch, out_dir, args.workers, args.quality, args.width
            )
            total_mb = sum(p.stat().st_size for p in pdfs) / (1024 * 1024)
            log.info("    PDF: %.1fMB در %d فایل", total_mb, len(pdfs))

            if bot is None:     # --no-upload
                mark(progress, slug, ch, "built")
                continue

            ok = True
            for i, pdf in enumerate(pdfs, 1):
                if not await send_pdf(bot, args.channel, pdf, info, ch, (i, len(pdfs))):
                    ok = False
                    break
                await asyncio.sleep(0.8)

            if ok:
                log.info("    ✓ ارسال شد")
                mark(progress, slug, ch, "done")
                if not args.keep_pdf:
                    for p in pdfs:
                        p.unlink(missing_ok=True)
            else:
                mark(progress, slug, ch, "failed")
                failed.append(f"{ch} (آپلود)")

        except ChapterError as e:
            log.warning("    ✗ %s", e)
            mark(progress, slug, ch, "failed")
            failed.append(f"{ch} ({e})")
        except Exception as e:  # noqa: BLE001
            log.exception("    ✗ خطای غیرمنتظره")
            mark(progress, slug, ch, "failed")
            failed.append(f"{ch} ({str(e)[:40]})")

        await asyncio.sleep(DELAY)

    # به‌جای یک پیام برای هر چپتر خراب، فقط یک گزارش نهایی
    if failed and bot and args.report_failures:
        text = (f"⚠️ <b>{html.escape(info['en'])}</b>\nچپترهای ناموفق:\n"
                + "\n".join(f"• {html.escape(f)}" for f in failed))[:4000]
        try:
            await with_retry(lambda: bot.send_message(chat_id=args.channel, text=text,
                                                       parse_mode=ParseMode.HTML))
        except Exception:  # noqa: BLE001
            pass
    return failed


async def main_async(args):
    if not args.no_upload and not args.baseline and (not args.token or not args.channel):
        log.error("❌ BOT_TOKEN و CHANNEL را تنظیم کن (متغیر محیطی یا --token / --channel)")
        return

    global SESSION
    SESSION = make_session(args.workers)
    OUTPUT_DIR.mkdir(exist_ok=True)
    global DEADLINE
    if args.max_minutes:
        DEADLINE = time.time() + args.max_minutes * 60
    progress, cached_map = await asyncio.to_thread(load_state)

    if args.slug:
        slugs = args.slug
    elif args.all:
        log.info("گرفتن لیست از آرشیو...")
        slugs = await asyncio.to_thread(get_all_slugs)
        log.info("%d مانهوا پیدا شد", len(slugs))
    else:
        log.error("❌ از --slug یا --all استفاده کن")
        return

    mapping = await asyncio.to_thread(load_map, slugs, cached_map, args.refresh)

    bot = None
    if not args.no_upload and not args.baseline:
        request = HTTPXRequest(connection_pool_size=2, read_timeout=300,
                               write_timeout=300, connect_timeout=60)
        bot = Bot(token=args.token, request=request)

    all_failed = {}
    for slug in slugs:
        if DEADLINE and time.time() > DEADLINE:
            break
        info = mapping.get(slug)
        if not info:
            log.warning("⚠ اطلاعات %s گرفته نشد، رد شد", slug)
            continue
        failed = await process_one_manhwa(bot, args, info, progress)
        if failed:
            all_failed[slug] = failed

    log.info("\n✅ تمام.")
    if all_failed:
        log.info("چپترهای ناموفق (با اجرای دوباره دوباره تلاش می‌شوند):")
        for slug, items in all_failed.items():
            log.info("  %s: %s", slug, ", ".join(items))
    log.info("پیشرفت: %s", "Supabase" if USE_DB else PROGRESS_FILE.resolve())


def parse_args():
    p = argparse.ArgumentParser(description="MangaBox → Telegram")
    p.add_argument("--slug", nargs="+", help="یک یا چند slug")
    p.add_argument("--all", action="store_true", help="همه‌ی آرشیو")
    p.add_argument("--chapters", nargs="+", help="فقط این چپترها (مثلاً 1 2 3.5)")
    p.add_argument("--token", default=os.getenv("BOT_TOKEN", ""))
    p.add_argument("--channel", default=os.getenv("CHANNEL", ""))
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--quality", type=int, default=JPEG_QUALITY)
    p.add_argument("--width", type=int, default=MAX_IMAGE_WIDTH)
    p.add_argument("--no-upload", action="store_true", help="فقط PDF بساز، آپلود نکن")
    p.add_argument("--keep-pdf", action="store_true", help="بعد از آپلود PDF را نگه دار")
    p.add_argument("--refresh", action="store_true", help="اطلاعات سایت را دوباره بگیر (نادیده گرفتن کش)")
    p.add_argument("--report-failures", action="store_true",
                   help="گزارش چپترهای ناموفق را در کانال بفرست")
    p.add_argument("--max-minutes", type=float, default=0,
                   help="بعد از این مدت (دقیقه) به‌صورت نرم متوقف شو (برای GitHub Actions)")
    p.add_argument("--baseline", action="store_true",
                   help="همه‌ی چپترهای فعلی را بدون ارسال «انجام‌شده» ثبت کن؛ از این به بعد فقط چپترهای جدید ارسال می‌شود")
    p.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args()
    # در CMD اگر set با گیومه نوشته شود، گیومه جزو مقدار می‌شود؛ اینجا پاکش می‌کنیم
    args.token = args.token.strip().strip("\"'")
    args.channel = args.channel.strip().strip("\"'")
    return args


def main():
    args = parse_args()
    if args.verbose:
        log.setLevel(logging.DEBUG)
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
