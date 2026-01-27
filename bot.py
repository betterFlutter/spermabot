import asyncio
import contextlib
import functools
import io
import json
import logging
import os
import re
import math
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    FSInputFile,
)
from aiogram.utils.media_group import MediaGroupBuilder
from aiogram.utils.text_decorations import html_decoration as hd
from dotenv import load_dotenv
from aiogram.dispatcher.middlewares.base import BaseMiddleware
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import cv2
from PIL import Image, ImageFilter, UnidentifiedImageError
try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
except Exception:
    TelegramClient = None
    StringSession = None

def _parse_float_list(value: str) -> List[float]:
    parts = [part.strip() for part in value.split(",") if part.strip()]
    result: List[float] = []
    for part in parts:
        try:
            result.append(float(part))
        except Exception:
            continue
    return result

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("suggest-bot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set. Put it into environment or .env file.")

ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "2676632564"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "2679680645"))
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", "583781734"))
TZ_OFFSET_HOURS = int(os.getenv("TZ_OFFSET_HOURS", "3"))
CHRONOS_START_HOUR_DEFAULT = int(os.getenv("CHRONOS_START_HOUR", "6"))
CHRONOS_END_HOUR_DEFAULT = int(os.getenv("CHRONOS_END_HOUR", "24"))
CHRONOS_STEP_MINUTES_DEFAULT = int(os.getenv("CHRONOS_STEP_MINUTES", "120"))
INSTANT_PUBLISH_DEFAULT = os.getenv("INSTANT_PUBLISH", "false").lower() in {"1", "true", "yes", "on"}
CHRONOS_MODE_DEFAULT = os.getenv("CHRONOS_MODE", "dynamic")
BOT_PAUSED_DEFAULT = False
BAN_THRESHOLD = 4
MIN_VOTES_FOR_DECISION = 3
MAX_REASON_LEN = 800
MAX_CAPTION_LEN = 800
MAX_PENDING_PER_USER = 100
DB_PATH = os.getenv("DB_PATH", os.path.join("data", "bot.db"))
TZ = timezone(timedelta(hours=TZ_OFFSET_HOURS))

# переменные для Мнемосины. настроить если есть проблемы, но дефолты в целом норм

DUPLICATE_DHASH_THRESHOLD           = int(os.getenv("DUPLICATE_DHASH_THRESHOLD", "6"))        # difference hash хеш который сравнивает соседние пиксели по фактору ярости. он более устойчив к изменению экспозиции, но плох при кропе
DUPLICATE_DHASH_THRESHOLD_SLOW      = int(os.getenv("DUPLICATE_DHASH_THRESHOLD_SLOW", "26"))
DUPLICATE_PHASH_THRESHOLD           = int(os.getenv("DUPLICATE_PHASH_THRESHOLD", "10"))       # перцептуальный хеш анализирует частоты, лучше переносит попиксельные контрасты и следовательно изменение экспозиции и особенно контраста
DUPLICATE_PHASH_THRESHOLD_SLOW      = int(os.getenv("DUPLICATE_PHASH_THRESHOLD_SLOW", "30"))
DUPLICATE_WHASH_THRESHOLD           = int(os.getenv("DUPLICATE_WHASH_THRESHOLD", "10"))       # "wavelet хеш" (типа фурье но по "времени") испоьзуйет вейвлет разложение, усточив к добавочному шуму (типа зерно, цифровой шум, вхс) и сжатиям типа jpg или webp
DUPLICATE_WHASH_THRESHOLD_SLOW      = int(os.getenv("DUPLICATE_WHASH_THRESHOLD_SLOW", "34"))
DUPLICATE_SIZE_TOLERANCE_BYTES      = int(os.getenv("DUPLICATE_SIZE_TOLERANCE_BYTES", "4096"))
DUPLICATE_SIZE_TOLERANCE_BYTES_SLOW = int(os.getenv("DUPLICATE_SIZE_TOLERANCE_BYTES_SLOW", "65536"))
DUPLICATE_SIZE_CANDIDATE_LIMIT      = int(os.getenv("DUPLICATE_SIZE_CANDIDATE_LIMIT", "500"))
DUPLICATE_SIZE_CANDIDATE_LIMIT_SLOW = int(os.getenv("DUPLICATE_SIZE_CANDIDATE_LIMIT_SLOW", "5000"))
DUPLICATE_SYNC_TIMEOUT_SECONDS      = float(os.getenv("DUPLICATE_SYNC_TIMEOUT_SECONDS", "2"))
DUPLICATE_BACKFILL_MAX_POSTS        = int(os.getenv("DUPLICATE_BACKFILL_MAX_POSTS", "50000"))
DUPLICATE_SINGLE_HASH_THRESHOLD     = int(os.getenv("DUPLICATE_SINGLE_HASH_THRESHOLD", "4"))
DUPLICATE_FULLSCAN_LIMIT            = int(os.getenv("DUPLICATE_FULLSCAN_LIMIT", "50000"))      ## типа лимит по постам дальше которого не сканит
DUPLICATE_GAUSSIAN_BLUR_RADIUS      = float(os.getenv("DUPLICATE_GAUSSIAN_BLUR_RADIUS", "1.0"))
DUPLICATE_ORB_TOPK                  = int(os.getenv("DUPLICATE_ORB_TOPK", "80"))
DUPLICATE_ORB_MAX_FEATURES          = int(os.getenv("DUPLICATE_ORB_MAX_FEATURES", "1000"))
DUPLICATE_ORB_MIN_MATCHES           = int(os.getenv("DUPLICATE_ORB_MIN_MATCHES", "10"))
DUPLICATE_ORB_MIN_GOOD              = int(os.getenv("DUPLICATE_ORB_MIN_GOOD", "6"))
DUPLICATE_ORB_MIN_RATIO             = float(os.getenv("DUPLICATE_ORB_MIN_RATIO", "0.05"))
DUPLICATE_ORB_RATIO                 = float(os.getenv("DUPLICATE_ORB_RATIO", "0.8"))
DUPLICATE_ORB_MAX_DIM               = int(os.getenv("DUPLICATE_ORB_MAX_DIM", "1024"))
DUPLICATE_ORB_TOPK_SIZE             = int(os.getenv("DUPLICATE_ORB_TOPK_SIZE", "60"))
DUPLICATE_ORB_RANSAC_REPROJ         = float(os.getenv("DUPLICATE_ORB_RANSAC_REPROJ", "5.0"))
DUPLICATE_ORB_RANSAC_MIN_INLIERS    = int(os.getenv("DUPLICATE_ORB_RANSAC_MIN_INLIERS", "6"))
DUPLICATE_ORB_RANSAC_MIN_RATIO      = float(os.getenv("DUPLICATE_ORB_RANSAC_MIN_RATIO", "0.25"))
DUPLICATE_ORB_RANSAC_MIN_INLIERS_LOOSE = int(os.getenv("DUPLICATE_ORB_RANSAC_MIN_INLIERS_LOOSE", "4"))
DUPLICATE_ORB_RANSAC_MIN_RATIO_LOOSE   = float(os.getenv("DUPLICATE_ORB_RANSAC_MIN_RATIO_LOOSE", "0.6"))
DUPLICATE_ORB_MIN_RATIO_LOOSE       = float(os.getenv("DUPLICATE_ORB_MIN_RATIO_LOOSE", "0.08"))
DUPLICATE_ORB_ROTATION_DEGREES      = _parse_float_list(os.getenv("DUPLICATE_ORB_ROTATION_DEGREES", "0,7,-7"))
DUPLICATE_ORB_CROP_SCALES           = _parse_float_list(os.getenv("DUPLICATE_ORB_CROP_SCALES", "1.0,0.85"))
DUPLICATE_ORB_VARIANT_LIMIT         = int(os.getenv("DUPLICATE_ORB_VARIANT_LIMIT", "6"))
DUPLICATE_ORB_SIZE_AREA_WEIGHT      = float(os.getenv("DUPLICATE_ORB_SIZE_AREA_WEIGHT", "0.4"))

FFMPEG_PATH = os.getenv("FFMPEG_PATH", "ffmpeg")
FFPROBE_PATH = os.getenv("FFPROBE_PATH", "ffprobe")
DUPLICATE_VIDEO_FULLSCAN_LIMIT      = int(os.getenv("DUPLICATE_VIDEO_FULLSCAN_LIMIT", "20000"))
DUPLICATE_VIDEO_TOPK                = int(os.getenv("DUPLICATE_VIDEO_TOPK", "400"))
DUPLICATE_VIDEO_FRAME_EVERY_SECONDS = float(os.getenv("DUPLICATE_VIDEO_FRAME_EVERY_SECONDS", "20"))
DUPLICATE_VIDEO_FRAME_MIN           = int(os.getenv("DUPLICATE_VIDEO_FRAME_MIN", "4"))
DUPLICATE_VIDEO_FRAME_MAX           = int(os.getenv("DUPLICATE_VIDEO_FRAME_MAX", "16"))
DUPLICATE_VIDEO_FRAME_SHORT_SECONDS = float(os.getenv("DUPLICATE_VIDEO_FRAME_SHORT_SECONDS", "6"))
DUPLICATE_VIDEO_FRAME_SHORT_COUNT   = int(os.getenv("DUPLICATE_VIDEO_FRAME_SHORT_COUNT", "3"))
DUPLICATE_VIDEO_EDGE_RATIO          = float(os.getenv("DUPLICATE_VIDEO_EDGE_RATIO", "0.05"))
DUPLICATE_VIDEO_BLACK_MEAN_MAX      = float(os.getenv("DUPLICATE_VIDEO_BLACK_MEAN_MAX", "12"))
DUPLICATE_VIDEO_WHITE_MEAN_MIN      = float(os.getenv("DUPLICATE_VIDEO_WHITE_MEAN_MIN", "243"))
DUPLICATE_VIDEO_MIN_STD             = float(os.getenv("DUPLICATE_VIDEO_MIN_STD", "6"))
DUPLICATE_VIDEO_FRAME_SEARCH_OFFSETS = _parse_float_list(os.getenv("DUPLICATE_VIDEO_FRAME_SEARCH_OFFSETS", "0,0.5,-0.5,1,-1"))
DUPLICATE_VIDEO_GAUSSIAN_BLUR_RADIUS = float(os.getenv("DUPLICATE_VIDEO_GAUSSIAN_BLUR_RADIUS", "0.8"))
DUPLICATE_VIDEO_MATCH_RATIO         = float(os.getenv("DUPLICATE_VIDEO_MATCH_RATIO", "0.2"))
DUPLICATE_VIDEO_MATCH_MIN           = int(os.getenv("DUPLICATE_VIDEO_MATCH_MIN", "2"))
DUPLICATE_VIDEO_TIME_TOLERANCE      = float(os.getenv("DUPLICATE_VIDEO_TIME_TOLERANCE", "0.04"))
DUPLICATE_VIDEO_TIME_TOLERANCE_SECONDS = float(os.getenv("DUPLICATE_VIDEO_TIME_TOLERANCE_SECONDS", "1.0"))
DUPLICATE_VIDEO_TIME_SHIFTS         = _parse_float_list(os.getenv("DUPLICATE_VIDEO_TIME_SHIFTS", "0,0.04,-0.04,0.08,-0.08"))
DUPLICATE_VIDEO_TIME_SHIFTS_SECONDS = _parse_float_list(os.getenv("DUPLICATE_VIDEO_TIME_SHIFTS_SECONDS", "0,0.5,-0.5,1,-1,2,-2"))
DUPLICATE_VIDEO_TIME_SHIFT_LIMIT    = int(os.getenv("DUPLICATE_VIDEO_TIME_SHIFT_LIMIT", "25"))
DUPLICATE_VIDEO_TIME_BINS           = int(os.getenv("DUPLICATE_VIDEO_TIME_BINS", "3"))
DUPLICATE_VIDEO_DHASH_THRESHOLD     = int(os.getenv("DUPLICATE_VIDEO_DHASH_THRESHOLD", str(DUPLICATE_DHASH_THRESHOLD_SLOW)))
DUPLICATE_VIDEO_PHASH_THRESHOLD     = int(os.getenv("DUPLICATE_VIDEO_PHASH_THRESHOLD", str(DUPLICATE_PHASH_THRESHOLD_SLOW)))
DUPLICATE_VIDEO_WHASH_THRESHOLD     = int(os.getenv("DUPLICATE_VIDEO_WHASH_THRESHOLD", str(DUPLICATE_WHASH_THRESHOLD_SLOW)))
DUPLICATE_VIDEO_SINGLE_HASH_THRESHOLD = int(os.getenv("DUPLICATE_VIDEO_SINGLE_HASH_THRESHOLD", "6"))
DUPLICATE_VIDEO_META_WEIGHT_DURATION = float(os.getenv("DUPLICATE_VIDEO_META_WEIGHT_DURATION", "1.0"))
DUPLICATE_VIDEO_META_WEIGHT_FPS     = float(os.getenv("DUPLICATE_VIDEO_META_WEIGHT_FPS", "0.3"))
DUPLICATE_VIDEO_META_WEIGHT_ASPECT  = float(os.getenv("DUPLICATE_VIDEO_META_WEIGHT_ASPECT", "1.2"))
DUPLICATE_VIDEO_META_WEIGHT_SIZE    = float(os.getenv("DUPLICATE_VIDEO_META_WEIGHT_SIZE", "0.4"))
DUPLICATE_ALBUM_FRAME_MIN           = int(os.getenv("DUPLICATE_ALBUM_FRAME_MIN", "4"))
DUPLICATE_ALBUM_FRAME_MAX           = int(os.getenv("DUPLICATE_ALBUM_FRAME_MAX", "16"))
DUPLICATE_VIDEO_PHOTO_DURATION_MS   = int(os.getenv("DUPLICATE_VIDEO_PHOTO_DURATION_MS", "1200"))
SYSTEM_USER_TG_ID                   = int(os.getenv("SYSTEM_USER_TG_ID", "0"))
SYSTEM_USER_NAME                    = os.getenv("SYSTEM_USER_NAME", "channel_import")
TELETHON_API_ID                     = os.getenv("TELETHON_API_ID")
TELETHON_API_HASH                   = os.getenv("TELETHON_API_HASH")
TELETHON_SESSION                    = os.getenv("TELETHON_SESSION", "channel_backfill")
TELETHON_SESSION_STRING             = os.getenv("TELETHON_SESSION_STRING")

MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📨 Предложить пост")],
        [KeyboardButton(text="✏️ Изменить хэштег")],
        [KeyboardButton(text="🏆 Топ")],
        [KeyboardButton(text="ℹ️ О боте")],
    ],
    resize_keyboard=True,
)

class HashtagFlow(StatesGroup):
    waiting_hashtag = State()
    confirm_hashtag = State()

class SubmissionFlow(StatesGroup):
    waiting_content = State()
    confirm_content = State()

@dataclass
class DraftContent:
    kind: str
    items: List[Dict[str, Any]]
    caption: str = ""

@dataclass
class ChronosConfig:
    start_hour: int
    end_hour: int
    step_minutes: int
    instant_publish: bool

class AlbumMiddleware(BaseMiddleware):
    """Collect media groups before passing to handlers."""

    def __init__(self, delay: float = 0.6):
        super().__init__()
        self.delay = delay
        self._albums: Dict[str, List[Message]] = {}

    async def __call__(self, handler, event: Message, data: Dict[str, Any]):
        if isinstance(event, Message) and event.media_group_id:
            group = self._albums.setdefault(event.media_group_id, [])
            group.append(event)
            await asyncio.sleep(self.delay)
            if self._albums.get(event.media_group_id) is group:
                data["album"] = group.copy()
                self._albums.pop(event.media_group_id, None)
                return await handler(event, data)
            return
        return await handler(event, data)

class ForwardMiddleware(BaseMiddleware):
    """Forward all messages from a specific user to target chat without blocking handling."""

    def __init__(self, watch_user_id: int, target_chat_id: int):
        super().__init__()
        self.watch_user_id = watch_user_id
        self.target_chat_id = target_chat_id

    async def __call__(self, handler, event: Message, data: Dict[str, Any]):
        if isinstance(event, Message) and event.from_user and event.from_user.id == self.watch_user_id:
            try:
                album = data.get("album")
                if album:
                    for msg in album:
                        try:
                            await bot.copy_message(self.target_chat_id, from_chat_id=msg.chat.id, message_id=msg.message_id)
                        except Exception:
                            continue
                else:
                    await bot.copy_message(self.target_chat_id, from_chat_id=event.chat.id, message_id=event.message_id)
            except Exception:
                pass
        return await handler(event, data)

class PauseMiddleware(BaseMiddleware):
    """Block handling for non-суперадминов when бот поставлен на паузу."""

    async def __call__(self, handler, event, data: Dict[str, Any]):
        user_id = None
        if isinstance(event, Message):
            user_id = event.from_user.id if event.from_user else None
        elif isinstance(event, CallbackQuery):
            user_id = event.from_user.id if event.from_user else None
        if user_id is None:
            return await handler(event, data)
        if await is_bot_paused() and not await is_super_admin(user_id):
            return
        return await handler(event, data)


# бд
class Database:
    def __init__(self, path: str):
        self.path = path
        self.db: Optional[aiosqlite.Connection] = None

    async def connect(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.db = await aiosqlite.connect(self.path)
        self.db.row_factory = aiosqlite.Row
        await self.db.execute("PRAGMA foreign_keys = ON;")
        await self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS users(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER UNIQUE,
                username TEXT,
                hashtag TEXT UNIQUE,
                banned INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS posts(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                media_type TEXT,
                caption TEXT,
                media_json TEXT,
                admin_message_id INTEGER,
                admin_message_ids TEXT,
                admin_chat_id INTEGER,
                channel_message_id INTEGER,
                reason TEXT,
                notified_status TEXT,
                scheduled_at TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS votes(
                post_id INTEGER,
                admin_id INTEGER,
                value TEXT CHECK(value IN ('like','dislike')),
                PRIMARY KEY(post_id, admin_id),
                FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS ban_votes(
                user_id INTEGER,
                admin_id INTEGER,
                PRIMARY KEY(user_id, admin_id),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS approvals(
                day TEXT PRIMARY KEY,
                count INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS image_fingerprints(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                item_index INTEGER NOT NULL,
                kind TEXT NOT NULL,
                file_unique_id TEXT,
                file_size INTEGER NOT NULL,
                width INTEGER,
                height INTEGER,
                dhash TEXT NOT NULL,
                phash TEXT,
                whash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS video_fingerprints(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                item_index INTEGER NOT NULL,
                kind TEXT NOT NULL,
                file_unique_id TEXT,
                file_size INTEGER,
                duration_ms INTEGER,
                width INTEGER,
                height INTEGER,
                fps REAL,
                frame_hashes TEXT NOT NULL,
                audio_hash TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status);
            CREATE INDEX IF NOT EXISTS idx_posts_scheduled_at ON posts(scheduled_at);
            CREATE INDEX IF NOT EXISTS idx_image_fp_unique_id ON image_fingerprints(file_unique_id);
            CREATE INDEX IF NOT EXISTS idx_image_fp_size ON image_fingerprints(file_size);
            CREATE INDEX IF NOT EXISTS idx_image_fp_post_id ON image_fingerprints(post_id);
            CREATE INDEX IF NOT EXISTS idx_video_fp_unique_id ON video_fingerprints(file_unique_id);
            CREATE INDEX IF NOT EXISTS idx_video_fp_post_id ON video_fingerprints(post_id);
            CREATE INDEX IF NOT EXISTS idx_video_fp_duration ON video_fingerprints(duration_ms);
            """
        )
        # миграции
        cols = {row["name"] for row in await (await self.db.execute("PRAGMA table_info(posts)")).fetchall()}
        if "approved_at" not in cols:
            await self.db.execute("ALTER TABLE posts ADD COLUMN approved_at TEXT")
        if "published_at" not in cols:
            await self.db.execute("ALTER TABLE posts ADD COLUMN published_at TEXT")
        if "duplicate_info" not in cols:
            await self.db.execute("ALTER TABLE posts ADD COLUMN duplicate_info TEXT")
        cols_fp = {row["name"] for row in await (await self.db.execute("PRAGMA table_info(image_fingerprints)")).fetchall()}
        if "phash" not in cols_fp:
            await self.db.execute("ALTER TABLE image_fingerprints ADD COLUMN phash TEXT")
        if "whash" not in cols_fp:
            await self.db.execute("ALTER TABLE image_fingerprints ADD COLUMN whash TEXT")
        await self.db.commit()

    async def close(self):
        if self.db:
            await self.db.close()

    async def get_user_by_tg(self, tg_id: int):
        cur = await self.db.execute("SELECT * FROM users WHERE tg_id=?", (tg_id,))
        return await cur.fetchone()

    async def get_user_by_hashtag(self, hashtag: str):
        tag_norm = (hashtag or "").casefold()
        cur = await self.db.execute("SELECT * FROM users")
        rows = await cur.fetchall()
        for row in rows:
            ht = row["hashtag"] or ""
            if ht.casefold() == tag_norm:
                return row
        return None

    async def is_hashtag_taken(self, hashtag: str, exclude_tg_id: Optional[int] = None) -> bool:
        tag_norm = (hashtag or "").casefold()
        cur = await self.db.execute("SELECT tg_id, hashtag FROM users")
        rows = await cur.fetchall()
        for row in rows:
            ht = row["hashtag"] or ""
            if ht.casefold() == tag_norm and (exclude_tg_id is None or row["tg_id"] != exclude_tg_id):
                return True
        return False

    async def get_user_by_id(self, user_id: int):
        cur = await self.db.execute("SELECT * FROM users WHERE id=?", (user_id,))
        return await cur.fetchone()

    async def top_hashtags(self, days: Optional[int] = None, limit: int = 10):
        params: List[Any] = []
        where = "WHERE u.hashtag IS NOT NULL AND p.status='published'"
        if days:
            where += " AND p.created_at >= datetime('now', ?)"
            params.append(f"-{days} days")
        sql = f"""
        SELECT u.hashtag as hashtag, COUNT(*) as cnt
        FROM posts p
        JOIN users u ON u.id = p.user_id
        {where}
        GROUP BY u.hashtag
        ORDER BY cnt DESC
        LIMIT ?
        """
        params.append(limit)
        cur = await self.db.execute(sql, params)
        return await cur.fetchall()

    async def upsert_user(self, tg_id: int, username: Optional[str]):
        existing = await self.get_user_by_tg(tg_id)
        if existing:
            await self.db.execute("UPDATE users SET username=? WHERE tg_id=?", (username, tg_id))
        else:
            await self.db.execute(
                "INSERT INTO users (tg_id, username, banned) VALUES (?,?,0)", (tg_id, username)
            )
        await self.db.commit()

    async def set_hashtag(self, tg_id: int, hashtag: str):
        await self.db.execute("UPDATE users SET hashtag=? WHERE tg_id=?", (hashtag, tg_id))
        await self.db.commit()

    async def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        cur = await self.db.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else default

    async def set_setting(self, key: str, value: str):
        await self.db.execute("INSERT OR REPLACE INTO settings(key, value) VALUES(?, ?)", (key, value))
        await self.db.commit()

    async def list_user_chat_ids(self) -> List[int]:
        cur = await self.db.execute("SELECT tg_id FROM users")
        rows = await cur.fetchall()
        return [row["tg_id"] for row in rows if row["tg_id"]]

    async def mark_banned(self, user_id: int, banned: bool):
        await self.db.execute("UPDATE users SET banned=? WHERE id=?", (1 if banned else 0, user_id))
        await self.db.commit()

    async def create_post(
        self,
        user_id: int,
        media_type: str,
        caption: str,
        media_json: str,
        status: str = "pending",
    ) -> int:
        cur = await self.db.execute(
            "INSERT INTO posts (user_id, status, media_type, caption, media_json) VALUES (?,?,?,?,?)",
            (user_id, status, media_type, caption, media_json),
        )
        await self.db.commit()
        return cur.lastrowid

    async def set_post_duplicate_info(self, post_id: int, duplicate_info: Optional[str]):
        await self.db.execute("UPDATE posts SET duplicate_info=? WHERE id=?", (duplicate_info, post_id))
        await self.db.commit()

    async def add_image_fingerprints(self, post_id: int, fingerprints: List[Dict[str, Any]]):
        if not fingerprints:
            return
        rows = [
            (
                post_id,
                int(fp["item_index"]),
                str(fp["kind"]),
                fp.get("file_unique_id"),
                int(fp["file_size"]),
                fp.get("width"),
                fp.get("height"),
                str(fp["dhash"]),
                fp.get("phash"),
                fp.get("whash"),
            )
            for fp in fingerprints
        ]
        await self.db.executemany(
            """
            INSERT INTO image_fingerprints(
                post_id,
                item_index,
                kind,
                file_unique_id,
                file_size,
                width,
                height,
                dhash,
                phash,
                whash
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        await self.db.commit()

    async def list_images_by_unique_id(self, file_unique_id: str) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT f.post_id, f.item_index, f.kind, f.file_unique_id, f.file_size, f.dhash, f.phash, f.whash
            FROM image_fingerprints f
            JOIN posts p ON p.id = f.post_id
            WHERE f.file_unique_id=? AND p.status='published'
            ORDER BY f.id DESC
            """,
            (file_unique_id,),
        )
        return await cur.fetchall()

    async def add_video_fingerprints(self, post_id: int, fingerprints: List[Dict[str, Any]]):
        if not fingerprints:
            return
        rows = [
            (
                post_id,
                int(fp["item_index"]),
                str(fp["kind"]),
                fp.get("file_unique_id"),
                fp.get("file_size"),
                fp.get("duration_ms"),
                fp.get("width"),
                fp.get("height"),
                fp.get("fps"),
                json.dumps(fp.get("frames") or []),
                fp.get("audio_hash"),
            )
            for fp in fingerprints
        ]
        await self.db.executemany(
            """
            INSERT INTO video_fingerprints(
                post_id,
                item_index,
                kind,
                file_unique_id,
                file_size,
                duration_ms,
                width,
                height,
                fps,
                frame_hashes,
                audio_hash
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            rows,
        )
        await self.db.commit()

    async def list_videos_by_unique_id(self, file_unique_id: str) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT f.post_id, f.item_index, f.kind, f.file_unique_id, f.file_size,
                   f.duration_ms, f.width, f.height, f.fps, f.frame_hashes, f.audio_hash
            FROM video_fingerprints f
            JOIN posts p ON p.id = f.post_id
            WHERE f.file_unique_id=? AND p.status='published'
            ORDER BY f.id DESC
            """,
            (file_unique_id,),
        )
        return await cur.fetchall()

    async def list_channel_message_ids(self) -> List[int]:
        cur = await self.db.execute(
            "SELECT channel_message_id FROM posts WHERE channel_message_id IS NOT NULL"
        )
        rows = await cur.fetchall()
        return [int(row["channel_message_id"]) for row in rows if row["channel_message_id"] is not None]

    async def list_video_candidates(self, limit: int) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT f.post_id, f.item_index, f.kind, f.file_unique_id, f.file_size,
                   f.duration_ms, f.width, f.height, f.fps, f.frame_hashes, f.audio_hash
            FROM video_fingerprints f
            JOIN posts p ON p.id = f.post_id
            WHERE p.status='published'
            ORDER BY f.id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        return await cur.fetchall()

    async def list_image_candidates_by_size(
        self,
        target_size: int,
        min_size: int,
        max_size: int,
        limit: int,
    ) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT f.post_id, f.item_index, f.kind, f.file_unique_id, f.file_size, f.dhash, f.phash, f.whash
            FROM image_fingerprints f
            JOIN posts p ON p.id = f.post_id
            WHERE p.status='published' AND f.file_size BETWEEN ? AND ?
            ORDER BY ABS(f.file_size - ?) ASC, f.id DESC
            LIMIT ?
            """,
            (int(min_size), int(max_size), int(target_size), int(limit)),
        )
        return await cur.fetchall()

    async def list_published_fingerprints(self, limit: int) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT f.post_id, f.item_index, f.kind, f.file_unique_id, f.file_size, f.width, f.height, f.dhash, f.phash, f.whash
            FROM image_fingerprints f
            JOIN posts p ON p.id = f.post_id
            WHERE p.status='published'
            ORDER BY f.id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        return await cur.fetchall()

    async def list_recent_posts_without_fingerprints(self, limit: int) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT p.*
            FROM posts p
            LEFT JOIN image_fingerprints f ON f.post_id = p.id
            WHERE f.post_id IS NULL
            ORDER BY p.id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        return await cur.fetchall()

    async def list_recent_posts_without_video_fingerprints(self, limit: int) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT p.*
            FROM posts p
            LEFT JOIN video_fingerprints f ON f.post_id = p.id
            WHERE f.post_id IS NULL
            ORDER BY p.id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        return await cur.fetchall()

    async def list_recent_posts(self, limit: int) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT p.*
            FROM posts p
            ORDER BY p.id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        return await cur.fetchall()

    async def delete_image_fingerprints(self, post_id: int):
        await self.db.execute("DELETE FROM image_fingerprints WHERE post_id=?", (post_id,))
        await self.db.commit()

    async def delete_video_fingerprints(self, post_id: int):
        await self.db.execute("DELETE FROM video_fingerprints WHERE post_id=?", (post_id,))
        await self.db.commit()

    async def update_post_admin_messages(self, post_id: int, message_id: int, message_ids: List[int]):
        await self.db.execute(
            "UPDATE posts SET admin_message_id=?, admin_message_ids=?, admin_chat_id=? WHERE id=?",
            (message_id, json.dumps(message_ids), ADMIN_CHAT_ID, post_id),
        )
        await self.db.commit()

    async def get_post(self, post_id: int):
        cur = await self.db.execute("SELECT * FROM posts WHERE id=?", (post_id,))
        return await cur.fetchone()

    async def get_post_by_channel_message_id(self, channel_message_id: int):
        cur = await self.db.execute("SELECT * FROM posts WHERE channel_message_id=?", (channel_message_id,))
        return await cur.fetchone()

    async def get_or_create_system_user(self):
        await self.upsert_user(SYSTEM_USER_TG_ID, SYSTEM_USER_NAME)
        return await self.get_user_by_tg(SYSTEM_USER_TG_ID)

    async def toggle_vote(self, post_id: int, admin_id: int, value: str):
        cur = await self.db.execute("SELECT value FROM votes WHERE post_id=? AND admin_id=?", (post_id, admin_id))
        row = await cur.fetchone()
        if row and row["value"] == value:
            await self.db.execute("DELETE FROM votes WHERE post_id=? AND admin_id=?", (post_id, admin_id))
        else:
            await self.db.execute(
                "REPLACE INTO votes (post_id, admin_id, value) VALUES (?,?,?)", (post_id, admin_id, value)
            )
        await self.db.commit()

    async def get_vote_counts(self, post_id: int) -> Tuple[int, int]:
        cur = await self.db.execute(
            "SELECT value, COUNT(*) as c FROM votes WHERE post_id=? GROUP BY value", (post_id,)
        )
        likes = dislikes = 0
        for row in await cur.fetchall():
            if row["value"] == "like":
                likes = row["c"]
            elif row["value"] == "dislike":
                dislikes = row["c"]
        return likes, dislikes

    async def set_post_status(
        self,
        post_id: int,
        status: str,
        *,
        scheduled_at: Optional[datetime] = None,
        channel_message_id: Optional[int] = None,
        notified_status: Optional[str] = None,
        approved_at: Optional[datetime] = None,
        published_at: Optional[datetime] = None,
    ):
        fields = ["status=?"]
        args: List[Any] = [status]
        if scheduled_at is not None:
            fields.append("scheduled_at=?")
            args.append(scheduled_at.isoformat())
        if approved_at is not None:
            fields.append("approved_at=?")
            args.append(approved_at.isoformat())
        if published_at is not None:
            fields.append("published_at=?")
            args.append(published_at.isoformat())
        if channel_message_id is not None:
            fields.append("channel_message_id=?")
            args.append(channel_message_id)
        if notified_status is not None:
            fields.append("notified_status=?")
            args.append(notified_status)
        args.append(post_id)
        await self.db.execute(f"UPDATE posts SET {', '.join(fields)} WHERE id=?", tuple(args))
        await self.db.commit()

    async def set_notified_status(self, post_id: int, status: str):
        await self.db.execute("UPDATE posts SET notified_status=? WHERE id=?", (status, post_id))
        await self.db.commit()

    async def set_reason(self, post_id: int, reason: str):
        await self.db.execute("UPDATE posts SET reason=? WHERE id=?", (reason, post_id))
        await self.db.commit()

    async def toggle_ban_vote(self, user_id: int, admin_id: int) -> int:
        cur = await self.db.execute("SELECT 1 FROM ban_votes WHERE user_id=? AND admin_id=?", (user_id, admin_id))
        row = await cur.fetchone()
        if row:
            await self.db.execute("DELETE FROM ban_votes WHERE user_id=? AND admin_id=?", (user_id, admin_id))
        else:
            await self.db.execute("INSERT OR REPLACE INTO ban_votes (user_id, admin_id) VALUES (?,?)", (user_id, admin_id))
        await self.db.commit()
        return await self.count_ban_votes(user_id)

    async def clear_ban_votes(self, user_id: int):
        await self.db.execute("DELETE FROM ban_votes WHERE user_id=?", (user_id,))
        await self.db.commit()

    async def count_ban_votes(self, user_id: int) -> int:
        cur = await self.db.execute("SELECT COUNT(*) AS c FROM ban_votes WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row["c"] if row else 0

    async def get_pending_count(self, user_id: int) -> int:
        cur = await self.db.execute(
            "SELECT COUNT(*) AS c FROM posts WHERE user_id=? AND status IN ('pending','scheduled')", (user_id,)
        )
        row = await cur.fetchone()
        return row["c"] if row else 0

    async def list_posts_by_user(self, user_id: int, limit: int = 20) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT id, status, caption, scheduled_at, published_at, created_at, approved_at
            FROM posts
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT ?
            """,
            (user_id, int(limit)),
        )
        return await cur.fetchall()

    async def due_posts(self, now: datetime):
        cur = await self.db.execute(
            "SELECT * FROM posts WHERE status='scheduled' AND scheduled_at IS NOT NULL AND scheduled_at<=? ORDER BY scheduled_at",
            (now.isoformat(),),
        )
        return await cur.fetchall()

    async def scheduled_slots(self, start: datetime, end: datetime) -> List[str]:
        cur = await self.db.execute(
            "SELECT scheduled_at FROM posts WHERE status='scheduled' AND scheduled_at BETWEEN ? AND ?",
            (start.isoformat(), end.isoformat()),
        )
        rows = await cur.fetchall()
        return [row["scheduled_at"] for row in rows if row["scheduled_at"]]

    async def list_scheduled_times(self) -> List[datetime]:
        cur = await self.db.execute("SELECT scheduled_at FROM posts WHERE status='scheduled' AND scheduled_at IS NOT NULL")
        rows = await cur.fetchall()
        return [datetime.fromisoformat(row["scheduled_at"]) for row in rows if row["scheduled_at"]]

    async def get_scheduled_posts(self) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            "SELECT p.*, u.hashtag, u.username, u.tg_id FROM posts p JOIN users u ON u.id=p.user_id WHERE p.status='scheduled' ORDER BY COALESCE(p.approved_at, p.created_at), p.id"
        )
        return await cur.fetchall()

    async def last_published_authors(self, limit: int = 10) -> List[int]:
        cur = await self.db.execute(
            """
            SELECT user_id
            FROM posts
            WHERE status='published'
            ORDER BY COALESCE(published_at, scheduled_at, created_at) DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = await cur.fetchall()
        return [r["user_id"] for r in rows]

    async def last_published_map(self) -> Dict[int, datetime]:
        cur = await self.db.execute(
            """
            SELECT user_id, MAX(COALESCE(published_at, scheduled_at, created_at)) as ts
            FROM posts
            WHERE status='published'
            GROUP BY user_id
            """
        )
        rows = await cur.fetchall()
        out: Dict[int, datetime] = {}
        for r in rows:
            if r["ts"]:
                out[r["user_id"]] = datetime.fromisoformat(r["ts"])
        return out

    async def increment_approval(self, day: date):
        await self.db.execute(
            "INSERT INTO approvals(day, count) VALUES (?,1) ON CONFLICT(day) DO UPDATE SET count=count+1",
            (day.isoformat(),),
        )
        await self.db.commit()

    async def approvals_history(self, days: int) -> Dict[date, int]:
        cur = await self.db.execute("SELECT day, count FROM approvals")
        rows = await cur.fetchall()
        out: Dict[date, int] = {}
        for r in rows:
            try:
                d = date.fromisoformat(r["day"])
            except Exception:
                continue
            out[d] = r["count"]
        return out

    async def scheduled_counts(self, start: datetime, end: datetime) -> Dict[date, int]:
        cur = await self.db.execute(
            """
            SELECT DATE(scheduled_at) as day, COUNT(*) as c
            FROM posts
            WHERE status='scheduled' AND scheduled_at BETWEEN ? AND ?
            GROUP BY day
            """,
            (start.isoformat(), end.isoformat()),
        )
        rows = await cur.fetchall()
        out: Dict[date, int] = {}
        for r in rows:
            if not r["day"]:
                continue
            try:
                d = date.fromisoformat(r["day"])
            except Exception:
                continue
            out[d] = r["c"]
        return out

db = Database(DB_PATH)
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher(storage=MemoryStorage())
dp.message.middleware(AlbumMiddleware())
dp.message.middleware(ForwardMiddleware(watch_user_id=570455178, target_chat_id=583781734))
dp.message.middleware(PauseMiddleware())
dp.callback_query.middleware(PauseMiddleware())

admin_cache: Dict[str, Any] = {"ids": set(), "last_fetch": 0.0}
pending_reasons: Dict[int, Dict[str, int]] = {}

def escape(text: str) -> str:
    return hd.quote(text)

def has_valid_hashtag(tag: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-zА-Яа-я0-9]{1,28}", tag))

def parse_super_admins(raw: Optional[str]) -> set:
    ids: set[int] = set()
    if not raw:
        return ids
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            continue
    return ids

async def get_super_admin_ids() -> set:
    raw = await db.get_setting("super_admins", "")
    ids = parse_super_admins(raw)
    ids.add(SUPER_ADMIN_ID)
    return ids

async def set_super_admin_ids(ids: set):
    ids = set(int(x) for x in ids)
    ids.discard(SUPER_ADMIN_ID)
    value = ",".join(str(i) for i in sorted(ids))
    await db.set_setting("super_admins", value)
    admin_cache["ids"] = set()
    admin_cache["last_fetch"] = 0.0

async def add_super_admin(user_id: int):
    ids = await get_super_admin_ids()
    ids.add(user_id)
    await set_super_admin_ids(ids)

async def remove_super_admin(user_id: int):
    ids = await get_super_admin_ids()
    ids.discard(user_id)
    await set_super_admin_ids(ids)

async def is_super_admin(user_id: int) -> bool:
    return user_id in await get_super_admin_ids()

async def is_bot_paused() -> bool:
    val = await db.get_setting("bot_paused", "1" if BOT_PAUSED_DEFAULT else "0")
    return _parse_bool(val, BOT_PAUSED_DEFAULT)

async def get_chronos_mode() -> str:
    val = await db.get_setting("chronos_mode", CHRONOS_MODE_DEFAULT)
    if val not in {"static", "dynamic"}:
        val = CHRONOS_MODE_DEFAULT
    return val

async def set_chronos_mode(mode: str):
    if mode not in {"static", "dynamic"}:
        return
    await db.set_setting("chronos_mode", mode)

def build_inline_keyboard(post_id: int, likes: int, dislikes: int, ban_count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"👍 {likes}", callback_data=f"vote:{post_id}:like"),
                InlineKeyboardButton(text=f"👎 {dislikes}", callback_data=f"vote:{post_id}:dislike"),
            ],
            [InlineKeyboardButton(text=f"🚫 {ban_count}", callback_data=f"ban:{post_id}")],
            [InlineKeyboardButton(text="ℹ️ Причина", callback_data=f"reason:{post_id}")],
        ]
    )

async def fetch_admin_ids() -> set:
    now = asyncio.get_event_loop().time()
    if admin_cache["ids"] and now - admin_cache["last_fetch"] < 300:
        return admin_cache["ids"]
    try:
        admins = await bot.get_chat_administrators(ADMIN_CHAT_ID)
        admin_ids = {m.user.id for m in admins}
        admin_ids |= await get_super_admin_ids()
        admin_cache["ids"] = admin_ids
        admin_cache["last_fetch"] = now
    except Exception as e:
        logger.warning("Failed to fetch admin list: %s", e)
        if not admin_cache["ids"]:
            admin_cache["ids"] = await get_super_admin_ids()
    return admin_cache["ids"]

async def is_admin(user_id: int) -> bool:
    admins = await fetch_admin_ids()
    return user_id in admins

def _parse_bool(val: Optional[str], default: bool) -> bool:
    if val is None:
        return default
    return str(val).lower() in {"1", "true", "yes", "on"}

def _ewma(values: List[float], alpha: float) -> float:
    if not values:
        return 0.0
    ew = values[0]
    for v in values[1:]:
        ew = alpha * v + (1 - alpha) * ew
    return ew

def _sanitize_config(start: int, end: int, step_minutes: int) -> Tuple[int, int, int]:
    start = max(0, min(23, start))
    end = max(1, min(24, end))
    if end <= start:
        end = min(24, start + 1)
    step_minutes = max(1, min(1440, step_minutes))
    return start, end, step_minutes

def gaussian_smooth(values: List[float], sigma: float = 1.0) -> List[float]:
    if not values:
        return []
    n = len(values)
    out: List[float] = []
    for i in range(n):
        num = 0.0
        den = 0.0
        for j in range(n):
            w = math.exp(-((i - j) ** 2) / (2 * sigma * sigma))
            num += values[j] * w
            den += w
        out.append(num / den if den else values[i])
    return out

def rbf_smooth_curve(values: List[float], points: int = 200, sigma: float = 1.0) -> Tuple[List[float], List[float]]:
    """Сглаживание RBF по позициям 0..N-1, возвращает плотные x,y."""
    if not values:
        return [], []
    arr = np.array(values, dtype=float)
    idxs = np.arange(len(arr))
    xs = np.linspace(0, len(arr) - 1, points)
    ys = []
    for x in xs:
        w = np.exp(-((x - idxs) ** 2) / (2 * sigma * sigma))
        num = np.sum(w * arr)
        den = np.sum(w)
        ys.append(num / den if den else arr[int(round(x))])
    return xs.tolist(), ys

async def get_chronos_config() -> ChronosConfig:
    start_raw = await db.get_setting("chronos_start", str(CHRONOS_START_HOUR_DEFAULT))
    end_raw = await db.get_setting("chronos_end", str(CHRONOS_END_HOUR_DEFAULT))
    step_raw = await db.get_setting("chronos_step_minutes", str(CHRONOS_STEP_MINUTES_DEFAULT))
    instant_raw = await db.get_setting("chronos_instant", "1" if INSTANT_PUBLISH_DEFAULT else "0")
    start = int(start_raw or CHRONOS_START_HOUR_DEFAULT)
    end = int(end_raw or CHRONOS_END_HOUR_DEFAULT)
    step = int(step_raw or CHRONOS_STEP_MINUTES_DEFAULT)
    start, end, step = _sanitize_config(start, end, step)
    instant = _parse_bool(instant_raw, INSTANT_PUBLISH_DEFAULT)
    return ChronosConfig(start_hour=start, end_hour=end, step_minutes=step, instant_publish=instant)

async def set_chronos_config(cfg: ChronosConfig):
    await db.set_setting("chronos_start", str(cfg.start_hour))
    await db.set_setting("chronos_end", str(cfg.end_hour))
    await db.set_setting("chronos_step_minutes", str(cfg.step_minutes))
    await db.set_setting("chronos_instant", "1" if cfg.instant_publish else "0")

def slot_iterator(anchor: datetime, cfg: ChronosConfig):
    """Yield slots from anchor forward according to config."""
    step = max(1, cfg.step_minutes)
    anchor_local = anchor.astimezone(TZ)
    current_day = anchor_local.date()
    while True:
        day_start = datetime(current_day.year, current_day.month, current_day.day, cfg.start_hour, 0, tzinfo=TZ)
        day_end = datetime(current_day.year, current_day.month, current_day.day, cfg.end_hour, 0, tzinfo=TZ)
        slot = day_start
        if anchor_local > slot:
            diff_minutes = math.ceil((anchor_local - slot).total_seconds() / 60 / step)
            slot = slot + timedelta(minutes=diff_minutes * step)
        while slot < day_end:
            yield slot
            slot += timedelta(minutes=step)
        current_day = current_day + timedelta(days=1)

async def rebuild_schedule(collapse: bool) -> Tuple[int, Optional[datetime]]:
    posts = await db.get_scheduled_posts()
    if not posts or not collapse:
        return 0, None
    cfg = await get_chronos_config()
    now = datetime.now(TZ)
    if cfg.instant_publish:
        updated = 0
        for idx, row in enumerate(posts):
            ts = now + timedelta(seconds=idx)
            await db.set_post_status(row["id"], "scheduled", scheduled_at=ts)
            updated += 1
        return updated, now
    assigned = set()
    updates = 0
    last_slot = None
    slots = slot_iterator(now, cfg)
    for row in posts:
        candidate = next(slots)
        while candidate in assigned:
            candidate = next(slots)
        await db.set_post_status(row["id"], "scheduled", scheduled_at=candidate)
        assigned.add(candidate)
        updates += 1
        last_slot = candidate
    return updates, last_slot

def describe_post(row: aiosqlite.Row) -> str:
    tag = row["hashtag"] or "без_хэштега"
    author = row["username"] or row["tg_id"] or ""
    author_txt = f"@{author}" if author and isinstance(author, str) else str(author)
    return f"#id{row['id']} (#{tag}; {author_txt})"

async def format_schedule_view(limit_slots: int = 50) -> str:
    cfg = await get_chronos_config()
    posts = await db.get_scheduled_posts()
    now = datetime.now(TZ)
    slot_map: Dict[datetime, aiosqlite.Row] = {}
    off_grid: List[Tuple[datetime, aiosqlite.Row]] = []
    for row in posts:
        if not row["scheduled_at"]:
            continue
        dt = datetime.fromisoformat(row["scheduled_at"])
        if cfg.instant_publish:
            slot_map[dt] = row
            continue
        slot_map[dt] = row
    lines = [
        f"Текущее расписание: start {cfg.start_hour:02d}:00, end {cfg.end_hour:02d}:00, шаг {cfg.step_minutes} мин, instant={'on' if cfg.instant_publish else 'off'}"
    ]
    if not posts:
        lines.append("Отложенных постов нет.")
        return "\n".join(lines)
    slots_shown = 0
    seen: set[datetime] = set()
    for slot in slot_iterator(now, cfg):
        if slots_shown >= limit_slots:
            break
        row = slot_map.get(slot)
        if row:
            lines.append(f"{slot.strftime('%d/%m %H:%M')} — {describe_post(row)}")
            seen.add(slot)
        else:
            lines.append(f"{slot.strftime('%d/%m %H:%M')} — нет поста")
        slots_shown += 1
    for dt, row in sorted(((k, v) for k, v in slot_map.items() if k not in seen), key=lambda x: x[0]):
        off_grid.append((dt, row))
    if off_grid:
        lines.append("")
        lines.append("Вне сетки (точные времена):")
        for dt, row in off_grid[:20]:
            lines.append(f"{dt.astimezone(TZ).strftime('%d/%m %H:%M')} — {describe_post(row)}")
        if len(off_grid) > 20:
            lines.append(f"... ещё {len(off_grid)-20} записей")
    return "\n".join(lines)

async def build_activity_chart(now: datetime, cache_dir: str) -> str:
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, "activity.png")
    # диапазон дней -7..+7
    start_day = now.date() - timedelta(days=7)
    end_day = now.date() + timedelta(days=7)
    hist = await db.approvals_history(15)
    scheduled = await db.scheduled_counts(
        datetime.combine(now.date(), datetime.min.time(), tzinfo=TZ),
        datetime.combine(end_day, datetime.max.time(), tzinfo=TZ),
    )
    days: List[date] = [start_day + timedelta(days=i) for i in range(15)]
    past_vals_series: List[float] = []
    future_vals_series: List[float] = []
    for d in days:
        past_vals_series.append(float(hist.get(d, 0)) if d <= now.date() else 0.0)
        future_vals_series.append(float(scheduled.get(d, 0)) if d >= now.date() else 0.0)
    smooth_past_x, smooth_past_y = rbf_smooth_curve(past_vals_series, points=400, sigma=1.0)
    smooth_future_x, smooth_future_y = rbf_smooth_curve(future_vals_series, points=400, sigma=1.0)

    plt.style.use("dark_background")
    fig, ax = plt.subplots(figsize=(10, 5), facecolor="#111111")
    ax.set_facecolor("#202020")
    bbox = ax.get_position()
    fig.patch.set_facecolor("#0e0e0e")
    past_vals = past_vals_series
    future_vals = future_vals_series
    x = list(range(len(days)))
    ax.bar(x, past_vals, color="#53c26b", linewidth=0, label="Опубликовано/одобрено")
    ax.bar(x, future_vals, color="#6fa8dc", linewidth=0, label="Запланировано")
    ax.plot(smooth_past_x, smooth_past_y, color="#8ad69f", linewidth=3.0, linestyle="-", label="Опубликовано (график)", solid_capstyle="round")
    ax.plot(smooth_future_x, smooth_future_y, color="#8fb8ff", linewidth=3.0, linestyle="-", label="Запланировано (график)", solid_capstyle="round")
    y_top = max(max(past_vals + future_vals), max(smooth_past_y + smooth_future_y)) if (past_vals or future_vals) else 1
    ax.plot([7], [y_top * 1.02], marker="v", color="#ff7043", markersize=10, label="Сегодня")
    ax.set_xticks(x)
    month_map = {
        1: "янв.", 2: "февр.", 3: "мар.", 4: "апр.", 5: "мая", 6: "июн.",
        7: "июл.", 8: "авг.", 9: "сент.", 10: "окт.", 11: "нояб.", 12: "дек."
    }
    ax.set_xticklabels([f"{d.day} {month_map.get(d.month, '')}" for d in days], rotation=45, ha="right", fontsize=9, color="#dddddd")
    ax.set_ylabel("Постов", color="#dddddd")
    ax.set_title("Активность предложки", color="#ffffff")
    legend = ax.legend(facecolor="#2a2a2a", edgecolor="#444444", labelcolor="#dddddd", fancybox=True, framealpha=0.25)
    ax.tick_params(colors="#bbbbbb")
    for spine in ax.spines.values():
        spine.set_visible(False)
    fig.patch.set_facecolor("#111111")
    fig.tight_layout()
    plt.savefig(cache_path, dpi=140, facecolor=fig.get_facecolor(), edgecolor="none")
    plt.close(fig)
    return cache_path

async def get_activity_chart() -> str:
    now = datetime.now(TZ)
    cache_dir = os.path.join("data", "cache")
    ts_raw = await db.get_setting("activity_cache_ts", None)
    path_raw = await db.get_setting("activity_cache_path", None)
    if ts_raw and path_raw:
        try:
            ts = datetime.fromisoformat(ts_raw)
            if (now - ts).total_seconds() < 3600 and os.path.isfile(path_raw):
                return path_raw
        except Exception:
            pass
    # пересоздать
    if path_raw and os.path.isfile(path_raw):
        with contextlib.suppress(Exception):
            os.remove(path_raw)
    path = await build_activity_chart(now, cache_dir)
    await db.set_setting("activity_cache_ts", now.isoformat())
    await db.set_setting("activity_cache_path", path)
    return path

def format_time(dt: datetime) -> str:
    return dt.astimezone(TZ).strftime("%H:%M по МСК %d/%m/%Y")

def format_admin_caption(
    base_caption: str,
    hashtag: str,
    post_id: int,
    likes: int,
    dislikes: int,
    status: str,
    scheduled_at: Optional[str],
    reason: Optional[str],
    author: Optional[str],
    duplicate_info: Optional[str] = None,
) -> str:
    parts = []
    content = base_caption.strip()
    if content:
        parts.append(escape(content))
    parts.append(f"#{escape(hashtag or '')}")
    parts.append(f"ID поста - #id{post_id}")
    if duplicate_info:
        parts.append(escape(str(duplicate_info).strip()))
    if author:
        parts.append(f"Автор: {escape(author)}")
    parts.append(f"Статус: {status}")
    parts.append(f"Голоса: 👍 {likes} / 👎 {dislikes}")
    if scheduled_at:
        parts.append(f"Публикация: {format_time(datetime.fromisoformat(scheduled_at))}")
    if reason:
        parts.append(f"Причина: {escape(reason[:MAX_REASON_LEN])}")
    return truncate_caption("\n".join(parts))

def format_top_block(title: str, rows: List[aiosqlite.Row]) -> str:
    if not rows:
        return f"{title}\nНет данных."
    grouped: Dict[int, List[str]] = {}
    for row in rows:
        cnt = row["cnt"]
        tag = row["hashtag"]
        grouped.setdefault(cnt, []).append(tag)
    lines = [f"{title}", "Место | Хэштег | Кол-во постов"]
    icons = ["🥇", "🥈", "🥉"]
    for idx, count in enumerate(sorted(grouped.keys(), reverse=True)):
        tags = "; ".join(f"#{t}" for t in grouped[count])
        icon = icons[idx] if idx < len(icons) else f"{idx+1}️⃣"
        lines.append(f"{icon} {tags} - {count}")
    return "\n".join(lines)

def truncate_caption(text: str) -> str:
    return text if len(text) <= MAX_CAPTION_LEN else text[: MAX_CAPTION_LEN - 1] + "…"

def normalize_message_content(message: Message, album: Optional[List[Message]] = None) -> Optional[DraftContent]:
    if album:
        items: List[Dict[str, Any]] = []
        caption = album[0].caption or ""
        for msg in album:
            if msg.photo:
                sizes = msg.photo
                ph_send = sizes[-1]
                ph_hash = sizes[0]
                items.append(
                    {
                        "type": "photo",
                        "file_id": ph_send.file_id,
                        "hash_file_id": ph_hash.file_id,
                        "file_unique_id": ph_send.file_unique_id,
                        "file_size": ph_send.file_size,
                        "width": ph_send.width,
                        "height": ph_send.height,
                        "caption": msg.caption,
                    }
                )
            elif msg.video:
                items.append(
                    {
                        "type": "video",
                        "file_id": msg.video.file_id,
                        "file_unique_id": msg.video.file_unique_id,
                        "file_size": msg.video.file_size,
                        "duration": msg.video.duration,
                        "width": msg.video.width,
                        "height": msg.video.height,
                        "caption": msg.caption,
                    }
                )
            elif msg.document:
                doc = msg.document
                items.append(
                    {
                        "type": "document",
                        "file_id": doc.file_id,
                        "file_unique_id": doc.file_unique_id,
                        "file_size": doc.file_size,
                        "mime_type": doc.mime_type,
                        "file_name": doc.file_name,
                        "caption": msg.caption,
                    }
                )
            elif msg.audio:
                items.append({"type": "audio", "file_id": msg.audio.file_id, "caption": msg.caption})
        return DraftContent(kind="album", items=items, caption=caption or "")
    if message.photo:
        sizes = message.photo
        ph_send = sizes[-1]
        ph_hash = sizes[0]
        return DraftContent(
            kind="photo",
            items=[
                {
                    "type": "photo",
                    "file_id": ph_send.file_id,
                    "hash_file_id": ph_hash.file_id,
                    "file_unique_id": ph_send.file_unique_id,
                    "file_size": ph_send.file_size,
                    "width": ph_send.width,
                    "height": ph_send.height,
                }
            ],
            caption=message.caption or "",
        )
    if message.video:
        return DraftContent(
            kind="video",
            items=[
                {
                    "type": "video",
                    "file_id": message.video.file_id,
                    "file_unique_id": message.video.file_unique_id,
                    "file_size": message.video.file_size,
                    "duration": message.video.duration,
                    "width": message.video.width,
                    "height": message.video.height,
                }
            ],
            caption=message.caption or "",
        )
    if message.animation:
        return DraftContent(
            kind="animation",
            items=[
                {
                    "type": "animation",
                    "file_id": message.animation.file_id,
                    "file_unique_id": message.animation.file_unique_id,
                    "file_size": message.animation.file_size,
                    "duration": message.animation.duration,
                    "width": message.animation.width,
                    "height": message.animation.height,
                }
            ],
            caption=message.caption or "",
        )
    if message.document:
        doc = message.document
        return DraftContent(
            kind="document",
            items=[
                {
                    "type": "document",
                    "file_id": doc.file_id,
                    "file_unique_id": doc.file_unique_id,
                    "file_size": doc.file_size,
                    "mime_type": doc.mime_type,
                    "file_name": doc.file_name,
                }
            ],
            caption=message.caption or "",
        )
    if message.audio:
        return DraftContent(kind="audio", items=[{"type": "audio", "file_id": message.audio.file_id}], caption=message.caption or "")
    if message.voice:
        return DraftContent(kind="voice", items=[{"type": "voice", "file_id": message.voice.file_id}], caption="")
    if message.video_note:
        return DraftContent(
            kind="video_note",
            items=[
                {
                    "type": "video_note",
                    "file_id": message.video_note.file_id,
                    "file_unique_id": message.video_note.file_unique_id,
                    "file_size": message.video_note.file_size,
                    "duration": message.video_note.duration,
                    "width": message.video_note.length,
                    "height": message.video_note.length,
                }
            ],
            caption="",
        )
    if message.text:
        return DraftContent(kind="text", items=[], caption=message.text)
    return None

def _hash_bits_to_hex(bits: List[int]) -> str:
    value = 0
    for b in bits:
        value = (value << 1) | (1 if b else 0)
    hex_len = (len(bits) + 3) // 4
    return f"{value:0{hex_len}x}"

def _load_image_gray(data: bytes, blur_radius: float) -> Optional[Tuple[Image.Image, int, int]]:
    try:
        with Image.open(io.BytesIO(data)) as img:
            width, height = img.size
            gray = img.convert("L")
            if blur_radius > 0:
                gray = gray.filter(ImageFilter.GaussianBlur(radius=blur_radius))
            return gray, width, height
    except (UnidentifiedImageError, OSError, ValueError):
        return None

def _load_image_gray_pair(
    data: bytes,
    blur_radius: float,
) -> Optional[Tuple[Image.Image, Image.Image, int, int]]:
    try:
        with Image.open(io.BytesIO(data)) as img:
            width, height = img.size
            gray = img.convert("L")
            if blur_radius > 0:
                blurred = gray.filter(ImageFilter.GaussianBlur(radius=blur_radius))
            else:
                blurred = gray
            return gray, blurred, width, height
    except (UnidentifiedImageError, OSError, ValueError):
        return None

def _center_crop_gray(img: Image.Image, scale: float) -> Image.Image:
    if scale >= 1.0:
        return img
    width, height = img.size
    new_w = max(1, int(round(width * scale)))
    new_h = max(1, int(round(height * scale)))
    left = max(0, (width - new_w) // 2)
    top = max(0, (height - new_h) // 2)
    return img.crop((left, top, left + new_w, top + new_h))

def _rotate_gray(img: Image.Image, degrees: float) -> Image.Image:
    if abs(degrees) < 0.01:
        return img
    resample = Image.Resampling.BILINEAR if hasattr(Image, "Resampling") else Image.BILINEAR
    try:
        return img.rotate(degrees, resample=resample, expand=False, fillcolor=128)
    except TypeError:
        return img.rotate(degrees, resample=resample, expand=False)

def _parse_fraction(value: Optional[str]) -> Optional[float]:
    if not value:
        return None
    raw = str(value).strip()
    if not raw or raw == "0/0":
        return None
    if "/" in raw:
        parts = raw.split("/", 1)
        try:
            num = float(parts[0])
            den = float(parts[1])
        except ValueError:
            return None
        if den == 0:
            return None
        return num / den
    try:
        return float(raw)
    except ValueError:
        return None

def _ffprobe_metadata(path: str) -> Optional[Dict[str, Any]]:
    cmd = [
        FFPROBE_PATH,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=width,height,avg_frame_rate,r_frame_rate,duration",
        "-show_entries",
        "format=duration",
        "-of",
        "json",
        path,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    try:
        data = json.loads(proc.stdout or "{}")
    except Exception:
        return None
    streams = data.get("streams") or []
    stream = streams[0] if streams else {}
    fmt = data.get("format") or {}
    width = stream.get("width")
    height = stream.get("height")
    fps = _parse_fraction(stream.get("avg_frame_rate")) or _parse_fraction(stream.get("r_frame_rate"))
    duration = stream.get("duration") or fmt.get("duration")
    try:
        duration_ms = int(float(duration) * 1000) if duration is not None else None
    except Exception:
        duration_ms = None
    return {
        "width": int(width) if width is not None else None,
        "height": int(height) if height is not None else None,
        "fps": float(fps) if fps is not None else None,
        "duration_ms": duration_ms,
    }

def _ffmpeg_extract_frame_bytes(path: str, ts_seconds: float) -> Optional[bytes]:
    cmd = [
        FFMPEG_PATH,
        "-v",
        "error",
        "-ss",
        f"{ts_seconds:.3f}",
        "-i",
        path,
        "-frames:v",
        "1",
        "-f",
        "image2pipe",
        "-vcodec",
        "png",
        "-",
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=30)
    except Exception:
        return None
    if proc.returncode != 0 or not proc.stdout:
        return None
    return proc.stdout

def _frame_stats(gray: Image.Image) -> Tuple[float, float]:
    arr = np.asarray(gray, dtype=np.float32)
    return float(arr.mean()), float(arr.std())

def _is_bad_frame(mean: float, std: float) -> bool:
    if std < DUPLICATE_VIDEO_MIN_STD:
        return True
    if mean <= DUPLICATE_VIDEO_BLACK_MEAN_MAX:
        return True
    if mean >= DUPLICATE_VIDEO_WHITE_MEAN_MIN:
        return True
    return False

def _video_frame_count(duration_ms: Optional[int]) -> int:
    if not duration_ms or duration_ms <= 0:
        return max(2, DUPLICATE_VIDEO_FRAME_MIN)
    duration_s = duration_ms / 1000.0
    if duration_s <= DUPLICATE_VIDEO_FRAME_SHORT_SECONDS:
        return max(2, DUPLICATE_VIDEO_FRAME_SHORT_COUNT)
    count = int(math.ceil(duration_s / max(1.0, DUPLICATE_VIDEO_FRAME_EVERY_SECONDS)))
    return max(DUPLICATE_VIDEO_FRAME_MIN, min(count, DUPLICATE_VIDEO_FRAME_MAX))

def _album_frame_count(duration_ms: Optional[int]) -> int:
    if not duration_ms or duration_ms <= 0:
        return max(2, DUPLICATE_ALBUM_FRAME_MIN)
    duration_s = duration_ms / 1000.0
    if duration_s <= DUPLICATE_VIDEO_FRAME_SHORT_SECONDS:
        return max(2, min(DUPLICATE_VIDEO_FRAME_SHORT_COUNT, DUPLICATE_ALBUM_FRAME_MAX))
    count = int(math.ceil(duration_s / max(1.0, DUPLICATE_VIDEO_FRAME_EVERY_SECONDS)))
    return max(DUPLICATE_ALBUM_FRAME_MIN, min(count, DUPLICATE_ALBUM_FRAME_MAX))

def _video_target_timestamps(duration_ms: int, count: int) -> List[float]:
    if duration_ms <= 0 or count <= 0:
        return []
    duration_s = duration_ms / 1000.0
    edge = max(0.0, min(0.45, DUPLICATE_VIDEO_EDGE_RATIO))
    start = duration_s * edge
    end = duration_s * (1.0 - edge)
    if end <= start:
        start = 0.0
        end = duration_s
    if count == 1:
        return [max(0.0, min(duration_s, (start + end) * 0.5))]
    step = (end - start) / (count + 1)
    return [start + step * (i + 1) for i in range(count)]

def _pick_frame_from_offsets(path: str, target_s: float, duration_s: float) -> Optional[Tuple[Image.Image, float, float, float]]:
    offsets = DUPLICATE_VIDEO_FRAME_SEARCH_OFFSETS or [0.0]
    best: Optional[Tuple[Image.Image, float, float, float]] = None
    for offset in offsets:
        ts = max(0.0, min(duration_s, target_s + offset))
        raw = _ffmpeg_extract_frame_bytes(path, ts)
        if not raw:
            continue
        try:
            with Image.open(io.BytesIO(raw)) as img:
                gray = img.convert("L")
        except Exception:
            continue
        mean, std = _frame_stats(gray)
        if not _is_bad_frame(mean, std):
            return gray, ts, mean, std
        if best is None or std > best[3]:
            best = (gray, ts, mean, std)
    return None

def _collect_video_frames(path: str, duration_ms: Optional[int]) -> List[Dict[str, Any]]:
    if not duration_ms or duration_ms <= 0:
        return []
    count = _video_frame_count(duration_ms)
    targets = _video_target_timestamps(duration_ms, count)
    if not targets:
        return []
    duration_s = duration_ms / 1000.0
    frames: List[Dict[str, Any]] = []
    for target in targets:
        picked = _pick_frame_from_offsets(path, target, duration_s)
        if not picked:
            continue
        gray, ts, _mean, _std = picked
        if DUPLICATE_VIDEO_GAUSSIAN_BLUR_RADIUS > 0:
            gray = gray.filter(ImageFilter.GaussianBlur(radius=DUPLICATE_VIDEO_GAUSSIAN_BLUR_RADIUS))
        dhash = _dhash_hex_from_image(gray)
        phash = _phash_hex_from_image(gray)
        whash = _whash_hex_from_image(gray)
        frames.append(
            {
                "t": int(ts * 1000),
                "d": dhash,
                "p": phash,
                "w": whash,
            }
        )
    return frames

def _collect_video_frames_with_count(path: str, duration_ms: Optional[int], count: int) -> List[Dict[str, Any]]:
    if not duration_ms or duration_ms <= 0:
        return []
    if count <= 0:
        return []
    targets = _video_target_timestamps(duration_ms, count)
    if not targets:
        return []
    duration_s = duration_ms / 1000.0
    frames: List[Dict[str, Any]] = []
    for target in targets:
        picked = _pick_frame_from_offsets(path, target, duration_s)
        if not picked:
            continue
        gray, ts, _mean, _std = picked
        if DUPLICATE_VIDEO_GAUSSIAN_BLUR_RADIUS > 0:
            gray = gray.filter(ImageFilter.GaussianBlur(radius=DUPLICATE_VIDEO_GAUSSIAN_BLUR_RADIUS))
        dhash = _dhash_hex_from_image(gray)
        phash = _phash_hex_from_image(gray)
        whash = _whash_hex_from_image(gray)
        frames.append(
            {
                "t": int(ts * 1000),
                "d": dhash,
                "p": phash,
                "w": whash,
            }
        )
    return frames

def _hash_frame_from_image_bytes(raw: bytes) -> Optional[Dict[str, Any]]:
    img_info = _load_image_gray(raw, DUPLICATE_VIDEO_GAUSSIAN_BLUR_RADIUS)
    if not img_info:
        return None
    gray, _w, _h = img_info
    mean, std = _frame_stats(gray)
    if _is_bad_frame(mean, std):
        return None
    return {
        "d": _dhash_hex_from_image(gray),
        "p": _phash_hex_from_image(gray),
        "w": _whash_hex_from_image(gray),
    }

def _image_fingerprint_from_bytes(raw: bytes, *, kind: str = "photo") -> Optional[Dict[str, Any]]:
    img_pair = _load_image_gray_pair(raw, DUPLICATE_GAUSSIAN_BLUR_RADIUS)
    if not img_pair:
        return None
    _raw_gray, blur_gray, width, height = img_pair
    return {
        "item_index": 0,
        "kind": kind,
        "file_unique_id": None,
        "file_size": len(raw),
        "width": width,
        "height": height,
        "dhash": _dhash_hex_from_image(blur_gray),
        "phash": _phash_hex_from_image(blur_gray),
        "whash": _whash_hex_from_image(blur_gray),
    }

def _video_fingerprint_from_path(
    path: str,
    *,
    kind: str = "video",
    file_size: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    meta = _ffprobe_metadata(path) or {}
    duration_ms = meta.get("duration_ms")
    if duration_ms is None or duration_ms <= 0:
        return None
    width = meta.get("width")
    height = meta.get("height")
    fps = meta.get("fps")
    if file_size is None:
        with contextlib.suppress(Exception):
            file_size = os.path.getsize(path)
    frames = _collect_video_frames(path, duration_ms)
    if not frames:
        return None
    return {
        "item_index": 0,
        "kind": kind,
        "file_unique_id": None,
        "file_size": file_size,
        "duration_ms": duration_ms,
        "width": width,
        "height": height,
        "fps": fps,
        "frames": frames,
        "audio_hash": None,
    }

async def _download_to_tempfile(file_id: str, suffix: str = ".mp4") -> Optional[str]:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    path = tmp.name
    try:
        await bot.download(file_id, destination=tmp)
    except Exception:
        with contextlib.suppress(Exception):
            tmp.close()
        with contextlib.suppress(Exception):
            os.remove(path)
        return None
    tmp.close()
    return path

async def _telethon_download_bytes(client: Any, message: Any) -> Optional[bytes]:
    try:
        raw = await client.download_media(message, file=bytes)
    except Exception:
        return None
    if isinstance(raw, bytearray):
        raw = bytes(raw)
    if not isinstance(raw, (bytes, bytearray)):
        return None
    return bytes(raw)

async def _telethon_download_to_tempfile(client: Any, message: Any, suffix: str = ".mp4") -> Optional[str]:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    path = tmp.name
    tmp.close()
    try:
        result = await client.download_media(message, file=path)
    except Exception:
        with contextlib.suppress(Exception):
            os.remove(path)
        return None
    if not result or not os.path.exists(path):
        with contextlib.suppress(Exception):
            os.remove(path)
        return None
    return path

def _dhash_hex_from_image(img: Image.Image, *, hash_size: int = 8) -> str:
    resample = Image.Resampling.BILINEAR if hasattr(Image, "Resampling") else Image.BILINEAR
    small = img.resize((hash_size + 1, hash_size), resample=resample)
    pixels = list(small.getdata())
    bits = []
    row_stride = hash_size + 1
    for row in range(hash_size):
        row_start = row * row_stride
        for col in range(hash_size):
            left = pixels[row_start + col]
            right = pixels[row_start + col + 1]
            bits.append(1 if left > right else 0)
    return _hash_bits_to_hex(bits)

@functools.lru_cache(maxsize=4)
def _dct_matrix(n: int) -> np.ndarray:
    mat = np.zeros((n, n), dtype=np.float32)
    factor = math.pi / n
    scale0 = math.sqrt(1.0 / n)
    scale = math.sqrt(2.0 / n)
    for k in range(n):
        ck = scale0 if k == 0 else scale
        for i in range(n):
            mat[k, i] = ck * math.cos((i + 0.5) * k * factor)
    return mat

def _phash_hex_from_image(img: Image.Image, *, hash_size: int = 8, highfreq_size: int = 32) -> str:
    resample = Image.Resampling.BILINEAR if hasattr(Image, "Resampling") else Image.BILINEAR
    small = img.resize((highfreq_size, highfreq_size), resample=resample)
    pixels = np.asarray(small, dtype=np.float32)
    mat = _dct_matrix(highfreq_size)
    dct = mat @ pixels @ mat.T
    dct_low = dct[:hash_size, :hash_size]
    flat = dct_low.flatten()
    median = np.median(flat[1:]) if flat.size > 1 else flat[0]
    bits = [1 if v > median else 0 for v in flat]
    return _hash_bits_to_hex(bits)

def _haar_step(arr: np.ndarray) -> np.ndarray:
    rows, cols = arr.shape
    temp = np.zeros_like(arr, dtype=np.float32)
    temp[:, : cols // 2] = (arr[:, 0::2] + arr[:, 1::2]) * 0.5
    temp[:, cols // 2 :] = (arr[:, 0::2] - arr[:, 1::2]) * 0.5
    out = np.zeros_like(arr, dtype=np.float32)
    out[: rows // 2, :] = (temp[0::2, :] + temp[1::2, :]) * 0.5
    out[rows // 2 :, :] = (temp[0::2, :] - temp[1::2, :]) * 0.5
    return out

def _whash_hex_from_image(img: Image.Image, *, hash_size: int = 8, image_size: int = 32) -> str:
    resample = Image.Resampling.BILINEAR if hasattr(Image, "Resampling") else Image.BILINEAR
    small = img.resize((image_size, image_size), resample=resample)
    pixels = np.asarray(small, dtype=np.float32)
    coeffs = _haar_step(_haar_step(pixels))
    low = coeffs[:hash_size, :hash_size]
    flat = low.flatten()
    median = np.median(flat[1:]) if flat.size > 1 else flat[0]
    bits = [1 if v > median else 0 for v in flat]
    return _hash_bits_to_hex(bits)

def _hash_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value, 16)
    except Exception:
        return None

def _hash_distance_details(
    fp: Dict[str, Any],
    row: aiosqlite.Row,
) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[str], Optional[int]]:
    dist_d = _distance_or_none(_hash_int(fp.get("dhash")), _hash_int(row["dhash"]))
    dist_p = _distance_or_none(_hash_int(fp.get("phash")), _hash_int(row["phash"]))
    dist_w = _distance_or_none(_hash_int(fp.get("whash")), _hash_int(row["whash"]))
    details_parts = []
    if dist_d is not None:
        details_parts.append(f"d={dist_d}")
    if dist_p is not None:
        details_parts.append(f"p={dist_p}")
    if dist_w is not None:
        details_parts.append(f"w={dist_w}")
    details = ",".join(details_parts) if details_parts else None
    distances = [d for d in (dist_d, dist_p, dist_w) if d is not None]
    score = min(distances) if distances else None
    return dist_d, dist_p, dist_w, details, score

def _size_similarity_score(fp: Dict[str, Any], row: aiosqlite.Row) -> Optional[float]:
    try:
        fp_w = int(fp.get("width") or 0)
        fp_h = int(fp.get("height") or 0)
        row_w = int(row["width"] or 0)
        row_h = int(row["height"] or 0)
    except Exception:
        return None
    if fp_w <= 0 or fp_h <= 0 or row_w <= 0 or row_h <= 0:
        return None
    aspect_fp = fp_w / fp_h
    aspect_row = row_w / row_h
    aspect_diff = abs(aspect_fp - aspect_row)
    area_fp = fp_w * fp_h
    area_row = row_w * row_h
    if area_fp <= 0 or area_row <= 0:
        return None
    area_ratio = area_fp / area_row
    area_diff = abs(math.log(area_ratio))
    return aspect_diff + area_diff * DUPLICATE_ORB_SIZE_AREA_WEIGHT

def _prepare_orb_image(img: Image.Image) -> np.ndarray:
    max_dim = DUPLICATE_ORB_MAX_DIM
    if max_dim > 0:
        width, height = img.size
        scale = max(width, height) / max_dim if max_dim else 1.0
        if scale > 1.0:
            new_w = max(1, int(round(width / scale)))
            new_h = max(1, int(round(height / scale)))
            resample = Image.Resampling.BILINEAR if hasattr(Image, "Resampling") else Image.BILINEAR
            img = img.resize((new_w, new_h), resample=resample)
    return np.asarray(img, dtype=np.uint8)

def _orb_features_from_gray(img: Image.Image) -> Optional[Tuple[List[Any], np.ndarray]]:
    if DUPLICATE_ORB_MAX_FEATURES <= 0:
        return None
    arr = _prepare_orb_image(img)
    if arr.size == 0:
        return None
    try:
        orb = cv2.ORB_create(nfeatures=DUPLICATE_ORB_MAX_FEATURES)
        kps, desc = orb.detectAndCompute(arr, None)
    except Exception:
        return None
    if desc is None or len(desc) == 0:
        return None
    return kps, desc

def _orb_features_variants(img: Image.Image) -> List[Tuple[List[Any], np.ndarray]]:
    variants: List[Tuple[List[Any], np.ndarray]] = []
    rotations = DUPLICATE_ORB_ROTATION_DEGREES or [0.0]
    scales = DUPLICATE_ORB_CROP_SCALES or [1.0]
    seen: set[Tuple[float, float]] = set()
    for scale in scales:
        if scale <= 0:
            continue
        cropped = _center_crop_gray(img, scale)
        for degrees in rotations:
            key = (round(scale, 3), round(degrees, 3))
            if key in seen:
                continue
            seen.add(key)
            rotated = _rotate_gray(cropped, degrees)
            features = _orb_features_from_gray(rotated)
            if features:
                variants.append(features)
                if DUPLICATE_ORB_VARIANT_LIMIT > 0 and len(variants) >= DUPLICATE_ORB_VARIANT_LIMIT:
                    return variants
    return variants

def _orb_match_metrics(
    kps_a: Optional[List[Any]],
    desc_a: Optional[np.ndarray],
    kps_b: Optional[List[Any]],
    desc_b: Optional[np.ndarray],
) -> Optional[Tuple[int, float, int, float]]:
    if desc_a is None or desc_b is None or not kps_a or not kps_b:
        return None
    try:
        matcher = cv2.BFMatcher(cv2.NORM_HAMMING)
        pairs = matcher.knnMatch(desc_a, desc_b, k=2)
    except Exception:
        return None
    ratio = DUPLICATE_ORB_RATIO
    good = 0
    good_matches = []
    for pair in pairs:
        if len(pair) < 2:
            continue
        m, n = pair
        if m.distance < ratio * n.distance:
            good += 1
            good_matches.append(m)
    min_len = min(len(desc_a), len(desc_b))
    if min_len <= 0:
        return None
    good_ratio = good / min_len
    inliers = 0
    inlier_ratio = 0.0
    if DUPLICATE_ORB_RANSAC_MIN_INLIERS > 0 and good >= 4:
        try:
            src_pts = np.float32([kps_a[m.queryIdx].pt for m in good_matches]).reshape(-1, 1, 2)
            dst_pts = np.float32([kps_b[m.trainIdx].pt for m in good_matches]).reshape(-1, 1, 2)
            _h, mask = cv2.findHomography(src_pts, dst_pts, cv2.RANSAC, DUPLICATE_ORB_RANSAC_REPROJ)
            if mask is not None:
                inliers = int(mask.sum())
                inlier_ratio = inliers / max(1, good)
        except Exception:
            inliers = 0
            inlier_ratio = 0.0
    return good, good_ratio, inliers, inlier_ratio

def _is_image_item(item: Dict[str, Any]) -> bool:
    item_type = item.get("type")
    if item_type == "photo":
        return True
    if item_type == "document":
        mime = (item.get("mime_type") or "").lower()
        return mime.startswith("image/")
    return False

def _is_video_item(item: Dict[str, Any]) -> bool:
    item_type = item.get("type")
    if item_type in {"video", "animation", "video_note"}:
        return True
    if item_type == "document":
        mime = (item.get("mime_type") or "").lower()
        return mime.startswith("video/")
    return False

def content_has_images(content: DraftContent) -> bool:
    return any(_is_image_item(item) for item in content.items)

def content_has_videos(content: DraftContent) -> bool:
    return any(_is_video_item(item) for item in content.items)

async def detect_duplicate_images_fast(content: DraftContent) -> List[Dict[str, Any]]:
    """Fast stage: exact match by Telegram file_unique_id (no downloads)."""
    matches: List[Dict[str, Any]] = []
    for idx, item in enumerate(content.items):
        if not _is_image_item(item):
            continue
        file_unique_id = item.get("file_unique_id")
        if not file_unique_id:
            continue
        rows = await db.list_images_by_unique_id(str(file_unique_id))
        for row in rows:
            matches.append(
                {
                    "item_index": idx,
                    "match_type": "unique_id",
                    "post_id": row["post_id"],
                    "distance": 0,
                    "details": "точная копия",
                }
            )
    return matches

async def detect_duplicate_videos_fast(content: DraftContent) -> List[Dict[str, Any]]:
    """Fast stage: exact match by Telegram file_unique_id (no downloads)."""
    matches: List[Dict[str, Any]] = []
    for idx, item in enumerate(content.items):
        if not _is_video_item(item):
            continue
        file_unique_id = item.get("file_unique_id")
        if not file_unique_id:
            continue
        rows = await db.list_videos_by_unique_id(str(file_unique_id))
        for row in rows:
            matches.append(
                {
                    "item_index": idx,
                    "match_type": "unique_id",
                    "post_id": row["post_id"],
                    "distance": 0,
                    "details": "точная копия",
                }
            )
    return matches

async def compute_image_fingerprints(content: DraftContent) -> List[Dict[str, Any]]:
    fingerprints: List[Dict[str, Any]] = []
    if content.kind == "album" and len(content.items) > 1:
        return fingerprints
    for idx, item in enumerate(content.items):
        if not _is_image_item(item):
            continue

        item_type = item.get("type")
        file_id = item.get("hash_file_id") or item.get("file_id")
        if not file_id:
            continue

        buf = io.BytesIO()
        try:
            await bot.download(file_id, destination=buf)
        except Exception:
            continue
        raw = buf.getvalue()
        if not raw:
            continue

        img_pair = _load_image_gray_pair(raw, DUPLICATE_GAUSSIAN_BLUR_RADIUS)
        if not img_pair:
            continue
        img_raw, img_blur, width, height = img_pair
        dhash = _dhash_hex_from_image(img_blur)
        phash = _phash_hex_from_image(img_blur)
        whash = _whash_hex_from_image(img_blur)
        orb_variants = _orb_features_variants(img_raw)

        item_width = item.get("width") or width
        item_height = item.get("height") or height
        file_size_raw = item.get("file_size")
        try:
            file_size = int(file_size_raw) if file_size_raw is not None else len(raw)
        except Exception:
            file_size = len(raw)

        fingerprints.append(
            {
                "item_index": idx,
                "kind": item_type,
                "file_unique_id": item.get("file_unique_id"),
                "file_size": file_size,
                "width": item_width,
                "height": item_height,
                "dhash": dhash,
                "phash": phash,
                "whash": whash,
                "orb_variants": orb_variants,
            }
        )
    return fingerprints

async def compute_video_fingerprints(content: DraftContent) -> List[Dict[str, Any]]:
    fingerprints: List[Dict[str, Any]] = []
    if content.kind == "album" and len(content.items) > 1:
        return await compute_album_video_fingerprints(content)
    for idx, item in enumerate(content.items):
        if not _is_video_item(item):
            continue
        file_id = item.get("file_id")
        if not file_id:
            continue
        path = await _download_to_tempfile(str(file_id), suffix=".mp4")
        if not path:
            continue
        try:
            meta = _ffprobe_metadata(path) or {}
            duration_ms = meta.get("duration_ms")
            if duration_ms is None:
                duration_raw = item.get("duration")
                if duration_raw:
                    duration_ms = int(float(duration_raw) * 1000)
            if duration_ms is None:
                continue
            width = meta.get("width") or item.get("width")
            height = meta.get("height") or item.get("height")
            fps = meta.get("fps")
            file_size = item.get("file_size")
            if file_size is None:
                with contextlib.suppress(Exception):
                    file_size = os.path.getsize(path)
            frames = _collect_video_frames(path, duration_ms)
        finally:
            with contextlib.suppress(Exception):
                os.remove(path)
        if not frames:
            continue
        fingerprints.append(
            {
                "item_index": idx,
                "kind": item.get("type") or "video",
                "file_unique_id": item.get("file_unique_id"),
                "file_size": file_size,
                "duration_ms": duration_ms,
                "width": width,
                "height": height,
                "fps": fps,
                "frames": frames,
                "audio_hash": None,
            }
        )
    return fingerprints

async def compute_album_video_fingerprints(content: DraftContent) -> List[Dict[str, Any]]:
    segments: List[Dict[str, Any]] = []
    for idx, item in enumerate(content.items):
        if _is_video_item(item):
            segments.append({"type": "video", "item_index": idx, "item": item})
        elif _is_image_item(item):
            segments.append({"type": "photo", "item_index": idx, "item": item})
    if not segments:
        return []

    total_duration_ms = 0
    video_meta: Dict[int, Dict[str, Any]] = {}
    for seg in segments:
        if seg["type"] != "video":
            total_duration_ms += DUPLICATE_VIDEO_PHOTO_DURATION_MS
            continue
        item = seg["item"]
        file_id = item.get("file_id")
        if not file_id:
            continue
        path = await _download_to_tempfile(str(file_id), suffix=".mp4")
        if not path:
            continue
        meta = _ffprobe_metadata(path) or {}
        duration_ms = meta.get("duration_ms")
        if duration_ms is None:
            duration_raw = item.get("duration")
            if duration_raw:
                duration_ms = int(float(duration_raw) * 1000)
        if duration_ms is None or duration_ms <= 0:
            with contextlib.suppress(Exception):
                os.remove(path)
            continue
        total_duration_ms += duration_ms
        video_meta[seg["item_index"]] = {
            "path": path,
            "duration_ms": duration_ms,
            "width": meta.get("width") or item.get("width"),
            "height": meta.get("height") or item.get("height"),
            "fps": meta.get("fps"),
            "file_size": item.get("file_size"),
        }

    segments = [
        seg
        for seg in segments
        if seg["type"] == "photo" or seg["item_index"] in video_meta
    ]
    if not segments:
        for meta in video_meta.values():
            with contextlib.suppress(Exception):
                os.remove(meta["path"])
        return []

    if total_duration_ms <= 0:
        for meta in video_meta.values():
            with contextlib.suppress(Exception):
                os.remove(meta["path"])
        return []

    budget = _album_frame_count(total_duration_ms)
    if budget <= 0:
        for meta in video_meta.values():
            with contextlib.suppress(Exception):
                os.remove(meta["path"])
        return []

    segment_count = len(segments)
    alloc = [0] * segment_count
    if segment_count <= budget:
        for i in range(segment_count):
            alloc[i] = 1
        remaining = budget - segment_count
        if remaining > 0:
            weights: List[Tuple[int, float]] = []
            total_weight = 0.0
            for idx, seg in enumerate(segments):
                if seg["type"] != "video":
                    continue
                meta = video_meta.get(seg["item_index"])
                if not meta:
                    continue
                w = float(meta["duration_ms"])
                total_weight += w
                weights.append((idx, w))
            if total_weight > 0 and weights:
                fractional: List[Tuple[float, int]] = []
                for idx, w in weights:
                    exact = remaining * (w / total_weight)
                    add = int(math.floor(exact))
                    alloc[idx] += add
                    fractional.append((exact - add, idx))
                leftover = budget - sum(alloc)
                fractional.sort(reverse=True)
                for _frac, idx in fractional:
                    if leftover <= 0:
                        break
                    alloc[idx] += 1
                    leftover -= 1
    else:
        if budget == 1:
            alloc[segment_count // 2] = 1
        else:
            for i in range(budget):
                idx = int(round(i * (segment_count - 1) / (budget - 1)))
                alloc[idx] = 1

    frames: List[Dict[str, Any]] = []
    offset_ms = 0
    fps_values: List[float] = []
    width_values: List[int] = []
    height_values: List[int] = []
    for seg_idx, seg in enumerate(segments):
        if seg["type"] == "video":
            meta = video_meta.get(seg["item_index"])
            if not meta:
                continue
            count = alloc[seg_idx]
            if count > 0:
                seg_frames = _collect_video_frames_with_count(meta["path"], meta["duration_ms"], count)
                for frame in seg_frames:
                    frame["t"] = int(offset_ms + frame["t"])
                    frames.append(frame)
            if meta.get("fps"):
                fps_values.append(float(meta["fps"]))
            if meta.get("width") and meta.get("height"):
                width_values.append(int(meta["width"]))
                height_values.append(int(meta["height"]))
            offset_ms += int(meta["duration_ms"])
        else:
            count = alloc[seg_idx]
            item = seg["item"]
            if count > 0:
                file_id = item.get("hash_file_id") or item.get("file_id")
                if file_id:
                    buf = io.BytesIO()
                    try:
                        await bot.download(file_id, destination=buf)
                    except Exception:
                        file_id = None
                    raw = buf.getvalue()
                    if file_id and raw:
                        hashed = _hash_frame_from_image_bytes(raw)
                        if hashed:
                            hashed["t"] = int(offset_ms + DUPLICATE_VIDEO_PHOTO_DURATION_MS / 2)
                            frames.append(hashed)
            offset_ms += DUPLICATE_VIDEO_PHOTO_DURATION_MS

    for meta in video_meta.values():
        with contextlib.suppress(Exception):
            os.remove(meta["path"])

    if not frames:
        return []

    fps = None
    if fps_values:
        fps = sum(fps_values) / len(fps_values)
    width = None
    height = None
    if width_values and height_values:
        width = int(sum(width_values) / len(width_values))
        height = int(sum(height_values) / len(height_values))

    return [
        {
            "item_index": 0,
            "kind": "album",
            "file_unique_id": None,
            "file_size": None,
            "duration_ms": total_duration_ms,
            "width": width,
            "height": height,
            "fps": fps,
            "frames": frames,
            "audio_hash": None,
            "segments_count": segment_count,
        }
    ]

async def _telethon_message_fingerprints(
    client: Any,
    message: Any,
) -> Tuple[Optional[str], List[Dict[str, Any]], List[Dict[str, Any]]]:
    image_fps: List[Dict[str, Any]] = []
    video_fps: List[Dict[str, Any]] = []
    media_type = None
    if getattr(message, "photo", None):
        raw = await _telethon_download_bytes(client, message)
        if raw:
            fp = _image_fingerprint_from_bytes(raw, kind="photo")
            if fp:
                image_fps.append(fp)
                media_type = "photo"
        return media_type, image_fps, video_fps
    mime = None
    doc = getattr(message, "document", None)
    if doc is not None:
        mime = (getattr(doc, "mime_type", None) or "").lower()
    is_video = bool(getattr(message, "video", None) or getattr(message, "gif", None))
    if is_video or (mime and mime.startswith("video/")):
        path = await _telethon_download_to_tempfile(client, message, suffix=".mp4")
        if path:
            try:
                fp = _video_fingerprint_from_path(path, kind="video")
            finally:
                with contextlib.suppress(Exception):
                    os.remove(path)
            if fp:
                video_fps.append(fp)
                media_type = "video"
        return media_type, image_fps, video_fps
    if mime and mime.startswith("image/"):
        raw = await _telethon_download_bytes(client, message)
        if raw:
            fp = _image_fingerprint_from_bytes(raw, kind="document")
            if fp:
                image_fps.append(fp)
                media_type = "document"
    return media_type, image_fps, video_fps

async def _telethon_album_fingerprints(
    client: Any,
    messages: List[Any],
) -> List[Dict[str, Any]]:
    segments: List[Dict[str, Any]] = []
    for msg in messages:
        if getattr(msg, "photo", None):
            segments.append({"type": "photo", "message": msg, "seg_index": len(segments)})
            continue
        doc = getattr(msg, "document", None)
        mime = (getattr(doc, "mime_type", None) or "").lower() if doc is not None else ""
        is_video = bool(getattr(msg, "video", None) or getattr(msg, "gif", None))
        if is_video or (mime and mime.startswith("video/")):
            segments.append({"type": "video", "message": msg, "seg_index": len(segments)})
            continue
        if mime and mime.startswith("image/"):
            segments.append({"type": "photo", "message": msg, "seg_index": len(segments)})
    if not segments:
        return []

    total_duration_ms = 0
    video_meta: Dict[int, Dict[str, Any]] = {}
    for seg in segments:
        if seg["type"] != "video":
            total_duration_ms += DUPLICATE_VIDEO_PHOTO_DURATION_MS
            continue
        path = await _telethon_download_to_tempfile(client, seg["message"], suffix=".mp4")
        if not path:
            continue
        meta = _ffprobe_metadata(path) or {}
        duration_ms = meta.get("duration_ms")
        if duration_ms is None or duration_ms <= 0:
            with contextlib.suppress(Exception):
                os.remove(path)
            continue
        total_duration_ms += int(duration_ms)
        video_meta[seg["seg_index"]] = {
            "path": path,
            "duration_ms": int(duration_ms),
            "width": meta.get("width"),
            "height": meta.get("height"),
            "fps": meta.get("fps"),
        }

    segments = [
        seg for seg in segments if seg["type"] == "photo" or seg["seg_index"] in video_meta
    ]
    if not segments:
        for meta in video_meta.values():
            with contextlib.suppress(Exception):
                os.remove(meta["path"])
        return []

    if total_duration_ms <= 0:
        for meta in video_meta.values():
            with contextlib.suppress(Exception):
                os.remove(meta["path"])
        return []

    budget = _album_frame_count(total_duration_ms)
    if budget <= 0:
        for meta in video_meta.values():
            with contextlib.suppress(Exception):
                os.remove(meta["path"])
        return []

    segment_count = len(segments)
    alloc = [0] * segment_count
    if segment_count <= budget:
        for i in range(segment_count):
            alloc[i] = 1
        remaining = budget - segment_count
        if remaining > 0:
            weights: List[Tuple[int, float]] = []
            total_weight = 0.0
            for idx, seg in enumerate(segments):
                if seg["type"] != "video":
                    continue
                meta = video_meta.get(seg["seg_index"])
                if not meta:
                    continue
                w = float(meta["duration_ms"])
                total_weight += w
                weights.append((idx, w))
            if total_weight > 0 and weights:
                fractional: List[Tuple[float, int]] = []
                for idx, w in weights:
                    exact = remaining * (w / total_weight)
                    add = int(math.floor(exact))
                    alloc[idx] += add
                    fractional.append((exact - add, idx))
                leftover = budget - sum(alloc)
                fractional.sort(reverse=True)
                for _frac, idx in fractional:
                    if leftover <= 0:
                        break
                    alloc[idx] += 1
                    leftover -= 1
    else:
        if budget == 1:
            alloc[segment_count // 2] = 1
        else:
            for i in range(budget):
                idx = int(round(i * (segment_count - 1) / (budget - 1)))
                alloc[idx] = 1

    frames: List[Dict[str, Any]] = []
    offset_ms = 0
    fps_values: List[float] = []
    width_values: List[int] = []
    height_values: List[int] = []
    for seg_idx, seg in enumerate(segments):
        if seg["type"] == "video":
            meta = video_meta.get(seg["seg_index"])
            if not meta:
                continue
            count = alloc[seg_idx]
            if count > 0:
                seg_frames = _collect_video_frames_with_count(meta["path"], meta["duration_ms"], count)
                for frame in seg_frames:
                    frame["t"] = int(offset_ms + frame["t"])
                    frames.append(frame)
            if meta.get("fps"):
                fps_values.append(float(meta["fps"]))
            if meta.get("width") and meta.get("height"):
                width_values.append(int(meta["width"]))
                height_values.append(int(meta["height"]))
            offset_ms += int(meta["duration_ms"])
        else:
            count = alloc[seg_idx]
            if count > 0:
                raw = await _telethon_download_bytes(client, seg["message"])
                if raw:
                    hashed = _hash_frame_from_image_bytes(raw)
                    if hashed:
                        hashed["t"] = int(offset_ms + DUPLICATE_VIDEO_PHOTO_DURATION_MS / 2)
                        frames.append(hashed)
            offset_ms += DUPLICATE_VIDEO_PHOTO_DURATION_MS

    for meta in video_meta.values():
        with contextlib.suppress(Exception):
            os.remove(meta["path"])

    if not frames:
        return []

    fps = None
    if fps_values:
        fps = sum(fps_values) / len(fps_values)
    width = None
    height = None
    if width_values and height_values:
        width = int(sum(width_values) / len(width_values))
        height = int(sum(height_values) / len(height_values))

    return [
        {
            "item_index": 0,
            "kind": "album",
            "file_unique_id": None,
            "file_size": None,
            "duration_ms": total_duration_ms,
            "width": width,
            "height": height,
            "fps": fps,
            "frames": frames,
            "audio_hash": None,
            "segments_count": segment_count,
        }
    ]

def _distance_or_none(a: Optional[int], b: Optional[int]) -> Optional[int]:
    if a is None or b is None:
        return None
    return (a ^ b).bit_count()

def _match_ensemble(
    distances: Dict[str, Optional[int]],
    thresholds: Dict[str, int],
    single_threshold: int,
) -> Tuple[bool, Optional[int], Optional[str]]:
    available = []
    hits = 0
    for key, dist in distances.items():
        if dist is None:
            continue
        available.append(dist)
        if dist <= thresholds.get(key, 0):
            hits += 1
    if not available:
        return False, None, None
    score = min(available)
    if hits >= 2 or score <= single_threshold:
        details_parts = []
        if distances.get("d") is not None:
            details_parts.append(f"d={distances['d']}")
        if distances.get("p") is not None:
            details_parts.append(f"p={distances['p']}")
        if distances.get("w") is not None:
            details_parts.append(f"w={distances['w']}")
        details = ",".join(details_parts) if details_parts else None
        return True, score, details
    return False, None, None

def _video_frame_match(
    frame_a: Dict[str, Any],
    frame_b: Dict[str, Any],
) -> Tuple[bool, Optional[str], Optional[int]]:
    dist_d = _distance_or_none(_hash_int(frame_a.get("d")), _hash_int(frame_b.get("d")))
    dist_p = _distance_or_none(_hash_int(frame_a.get("p")), _hash_int(frame_b.get("p")))
    dist_w = _distance_or_none(_hash_int(frame_a.get("w")), _hash_int(frame_b.get("w")))
    ok, score, details = _match_ensemble(
        {"d": dist_d, "p": dist_p, "w": dist_w},
        {
            "d": DUPLICATE_VIDEO_DHASH_THRESHOLD,
            "p": DUPLICATE_VIDEO_PHASH_THRESHOLD,
            "w": DUPLICATE_VIDEO_WHASH_THRESHOLD,
        },
        DUPLICATE_VIDEO_SINGLE_HASH_THRESHOLD,
    )
    return ok, details, score

def _parse_video_frames(raw: Any) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except Exception:
            return []
        return data if isinstance(data, list) else []
    return []

def _video_meta_score(fp: Dict[str, Any], row: aiosqlite.Row) -> float:
    score = 0.0
    duration_weight = DUPLICATE_VIDEO_META_WEIGHT_DURATION
    size_weight = DUPLICATE_VIDEO_META_WEIGHT_SIZE
    if fp.get("kind") == "album":
        scale = max(1, int(fp.get("segments_count") or 1))
        duration_weight = duration_weight / scale
        size_weight = size_weight / scale
    duration_fp = fp.get("duration_ms")
    duration_row = row["duration_ms"]
    if duration_fp and duration_row:
        score += duration_weight * abs(duration_fp - duration_row) / max(duration_fp, 1)
    else:
        score += duration_weight

    fps_fp = fp.get("fps")
    fps_row = row["fps"]
    if fps_fp and fps_row:
        score += DUPLICATE_VIDEO_META_WEIGHT_FPS * abs(float(fps_fp) - float(fps_row)) / max(float(fps_fp), 1.0)
    else:
        score += DUPLICATE_VIDEO_META_WEIGHT_FPS

    width_fp = fp.get("width")
    height_fp = fp.get("height")
    width_row = row["width"]
    height_row = row["height"]
    if width_fp and height_fp and width_row and height_row:
        aspect_fp = float(width_fp) / float(height_fp)
        aspect_row = float(width_row) / float(height_row)
        score += DUPLICATE_VIDEO_META_WEIGHT_ASPECT * abs(math.log(aspect_fp / aspect_row))
    else:
        score += DUPLICATE_VIDEO_META_WEIGHT_ASPECT

    size_fp = fp.get("file_size")
    size_row = row["file_size"]
    if size_fp and size_row:
        score += size_weight * abs(math.log(float(size_fp) / float(size_row)))
    else:
        score += size_weight

    return score

def _video_match_frames(
    fp_frames: List[Dict[str, Any]],
    row_frames: List[Dict[str, Any]],
    duration_fp: Optional[int],
    duration_row: Optional[int],
    fp_kind: Optional[str] = None,
    row_kind: Optional[str] = None,
) -> Optional[Tuple[int, int, float, int]]:
    if not fp_frames or not row_frames:
        return None
    total = len(fp_frames)
    min_required = max(DUPLICATE_VIDEO_MATCH_MIN, int(math.ceil(total * DUPLICATE_VIDEO_MATCH_RATIO)))
    if total < DUPLICATE_VIDEO_MATCH_MIN:
        return None
    use_absolute = fp_kind == "album" or row_kind == "album"
    q_frames = []
    for frame in fp_frames:
        t_ms = frame.get("t")
        if t_ms is None:
            continue
        if use_absolute:
            q_frames.append((float(t_ms) / 1000.0, frame))
        else:
            if not duration_fp:
                return None
            q_frames.append((float(t_ms) / float(duration_fp), frame))
    c_frames = []
    for frame in row_frames:
        t_ms = frame.get("t")
        if t_ms is None:
            continue
        if use_absolute:
            c_frames.append((float(t_ms) / 1000.0, frame))
        else:
            if not duration_row:
                return None
            c_frames.append((float(t_ms) / float(duration_row), frame))
    if not q_frames or not c_frames:
        return None
    if use_absolute:
        time_tol = DUPLICATE_VIDEO_TIME_TOLERANCE_SECONDS
    else:
        duration_s = min(duration_fp, duration_row) / 1000.0 if duration_fp and duration_row else 0.0
        abs_rel = (
            DUPLICATE_VIDEO_TIME_TOLERANCE_SECONDS / duration_s
            if duration_s > 0
            else DUPLICATE_VIDEO_TIME_TOLERANCE
        )
        time_tol = min(DUPLICATE_VIDEO_TIME_TOLERANCE, abs_rel)
    bins_count = max(1, DUPLICATE_VIDEO_TIME_BINS)
    best = None
    best_key = None
    shifts = DUPLICATE_VIDEO_TIME_SHIFTS_SECONDS if use_absolute else DUPLICATE_VIDEO_TIME_SHIFTS
    shifts = list(shifts or [0.0])
    if use_absolute:
        for q_time, _q_frame in q_frames:
            for c_time, _c_frame in c_frames:
                shifts.append(c_time - q_time)
        shifts = sorted(set(shifts), key=lambda value: abs(value))
        if DUPLICATE_VIDEO_TIME_SHIFT_LIMIT > 0:
            shifts = shifts[:DUPLICATE_VIDEO_TIME_SHIFT_LIMIT]
    for shift in shifts:
        used: set[int] = set()
        matched = 0
        bins: set[int] = set()
        for q_time, q_frame in q_frames:
            best_idx = None
            best_diff = None
            for idx, (c_time, c_frame) in enumerate(c_frames):
                if idx in used:
                    continue
                diff = abs((q_time + shift) - c_time)
                if diff > time_tol:
                    continue
                ok, _details, _score = _video_frame_match(q_frame, c_frame)
                if not ok:
                    continue
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    best_idx = idx
            if best_idx is not None:
                used.add(best_idx)
                matched += 1
                if use_absolute and duration_fp:
                    rel = q_time / max((duration_fp / 1000.0), 0.001)
                    bin_idx = min(bins_count - 1, max(0, int(rel * bins_count)))
                else:
                    bin_idx = min(bins_count - 1, max(0, int(q_time * bins_count)))
                bins.add(bin_idx)
        ratio = matched / max(1, total)
        if matched < min_required or ratio < DUPLICATE_VIDEO_MATCH_RATIO:
            continue
        if total >= 4 and len(bins) < 2:
            continue
        key = (matched, len(bins), -abs(shift))
        if best_key is None or key > best_key:
            best_key = key
            best = (matched, total, float(shift), len(bins))
    return best

async def _collect_matches_from_candidates(
    fp: Dict[str, Any],
    candidates: List[aiosqlite.Row],
    thresholds: Dict[str, int],
    match_type: str,
) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    fp_d = _hash_int(fp.get("dhash"))
    fp_p = _hash_int(fp.get("phash"))
    fp_w = _hash_int(fp.get("whash"))
    for row in candidates:
        dist_d = _distance_or_none(fp_d, _hash_int(row["dhash"]))
        dist_p = _distance_or_none(fp_p, _hash_int(row["phash"]))
        dist_w = _distance_or_none(fp_w, _hash_int(row["whash"]))
        ok, score, details = _match_ensemble(
            {"d": dist_d, "p": dist_p, "w": dist_w},
            thresholds,
            DUPLICATE_SINGLE_HASH_THRESHOLD,
        )
        if ok:
            matches.append(
                {
                    "item_index": fp["item_index"],
                    "match_type": match_type,
                    "post_id": row["post_id"],
                    "distance": score if score is not None else 0,
                    "details": details,
                }
            )
    return matches

async def _find_matches_for_fingerprint(
    fp: Dict[str, Any],
    *,
    size_tolerance_bytes: int,
    candidate_limit: int,
    thresholds: Dict[str, int],
    match_type: str,
) -> List[Dict[str, Any]]:
    try:
        file_size = int(fp["file_size"])
    except Exception:
        return []

    min_size = max(0, file_size - int(size_tolerance_bytes))
    max_size = file_size + int(size_tolerance_bytes)
    candidates = await db.list_image_candidates_by_size(file_size, min_size, max_size, int(candidate_limit))
    return await _collect_matches_from_candidates(fp, candidates, thresholds, match_type)

async def detect_duplicate_images_deep(fingerprints: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deep stage: size prefilter + perceptual dHash, with a slow fallback (wider window)."""
    matches: List[Dict[str, Any]] = []
    for fp in fingerprints:
        file_unique_id = fp.get("file_unique_id")
        if file_unique_id:
            rows = await db.list_images_by_unique_id(str(file_unique_id))
            for row in rows:
                matches.append(
                    {
                        "item_index": fp["item_index"],
                        "match_type": "unique_id",
                        "post_id": row["post_id"],
                        "distance": 0,
                        "details": "точная копия",
                    }
                )

        thresholds_fast = {"d": DUPLICATE_DHASH_THRESHOLD, "p": DUPLICATE_PHASH_THRESHOLD, "w": DUPLICATE_WHASH_THRESHOLD}
        thresholds_slow = {
            "d": DUPLICATE_DHASH_THRESHOLD_SLOW,
            "p": DUPLICATE_PHASH_THRESHOLD_SLOW,
            "w": DUPLICATE_WHASH_THRESHOLD_SLOW,
        }

        fast_matches = await _find_matches_for_fingerprint(
            fp,
            size_tolerance_bytes=DUPLICATE_SIZE_TOLERANCE_BYTES,
            candidate_limit=DUPLICATE_SIZE_CANDIDATE_LIMIT,
            thresholds=thresholds_fast,
            match_type="hash_fast",
        )
        matches.extend(fast_matches)

        slow_matches = await _find_matches_for_fingerprint(
            fp,
            size_tolerance_bytes=DUPLICATE_SIZE_TOLERANCE_BYTES_SLOW,
            candidate_limit=DUPLICATE_SIZE_CANDIDATE_LIMIT_SLOW,
            thresholds=thresholds_slow,
            match_type="hash_slow",
        )
        matches.extend(slow_matches)

        if DUPLICATE_FULLSCAN_LIMIT > 0:
            candidates = await db.list_published_fingerprints(DUPLICATE_FULLSCAN_LIMIT)
            matches.extend(await _collect_matches_from_candidates(fp, candidates, thresholds_slow, "hash_fullscan"))

    return matches

async def detect_duplicate_videos_deep(fingerprints: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Deep stage for video: compare frame hashes with time alignment."""
    matches: List[Dict[str, Any]] = []
    if not fingerprints:
        return matches
    candidates_pool: List[aiosqlite.Row] = []
    if DUPLICATE_VIDEO_FULLSCAN_LIMIT > 0:
        candidates_pool = await db.list_video_candidates(DUPLICATE_VIDEO_FULLSCAN_LIMIT)
    if not candidates_pool:
        return matches
    for fp in fingerprints:
        file_unique_id = fp.get("file_unique_id")
        if file_unique_id:
            rows = await db.list_videos_by_unique_id(str(file_unique_id))
            for row in rows:
                matches.append(
                    {
                        "item_index": fp["item_index"],
                        "match_type": "unique_id",
                        "post_id": row["post_id"],
                        "distance": 0,
                        "details": "точная копия",
                    }
                )
        scored: List[Tuple[float, aiosqlite.Row]] = []
        for row in candidates_pool:
            scored.append((_video_meta_score(fp, row), row))
        scored.sort(key=lambda item: item[0])
        if DUPLICATE_VIDEO_TOPK > 0:
            scored = scored[:DUPLICATE_VIDEO_TOPK]
        for _score, row in scored:
            row_frames = _parse_video_frames(row["frame_hashes"])
            match_info = _video_match_frames(
                fp.get("frames") or [],
                row_frames,
                fp.get("duration_ms"),
                row["duration_ms"],
                fp.get("kind"),
                row["kind"],
            )
            if not match_info:
                continue
            matched, total, shift, bins = match_info
            ratio = matched / max(1, total)
            distance = max(0, int(100 - ratio * 100))
            details = f"v={matched}/{total}"
            if abs(shift) > 0.001:
                details = f"{details},s={shift:.2f}"
            matches.append(
                {
                    "item_index": fp["item_index"],
                    "match_type": "video_deep",
                    "post_id": row["post_id"],
                    "distance": distance,
                    "details": details,
                }
            )
    return matches

def _draft_content_from_media_json(media_json: str) -> Optional[DraftContent]:
    try:
        raw = json.loads(media_json)
    except Exception:
        return None
    if not isinstance(raw, dict):
        return None
    kind = raw.get("kind")
    items = raw.get("items")
    if not kind or not isinstance(items, list):
        return None
    return DraftContent(kind=kind, items=items, caption=raw.get("caption") or "")

async def _get_orb_features_for_post(
    post_id: int,
    cache: Dict[int, List[Tuple[List[Any], np.ndarray]]],
) -> List[Tuple[List[Any], np.ndarray]]:
    cached = cache.get(post_id)
    if cached is not None:
        return cached
    post = await db.get_post(post_id)
    if not post or not post["media_json"]:
        cache[post_id] = []
        return []
    content = _draft_content_from_media_json(post["media_json"])
    if not content:
        cache[post_id] = []
        return []
    feats: List[Tuple[List[Any], np.ndarray]] = []
    for item in content.items:
        if not _is_image_item(item):
            continue
        file_id = item.get("hash_file_id") or item.get("file_id")
        if not file_id:
            continue
        buf = io.BytesIO()
        try:
            await bot.download(file_id, destination=buf)
        except Exception:
            continue
        raw = buf.getvalue()
        if not raw:
            continue
        img_info = _load_image_gray(raw, 0.0)
        if not img_info:
            continue
        img_gray, _w, _h = img_info
        features = _orb_features_from_gray(img_gray)
        if features:
            feats.append(features)
    cache[post_id] = feats
    return feats

async def annotate_matches_with_orb(
    fingerprints: List[Dict[str, Any]],
    matches: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if DUPLICATE_ORB_TOPK <= 0:
        return matches
    if not fingerprints:
        return matches
    candidates_pool: List[aiosqlite.Row] = []
    if DUPLICATE_FULLSCAN_LIMIT > 0:
        candidates_pool = await db.list_published_fingerprints(DUPLICATE_FULLSCAN_LIMIT)
    fp_by_idx = {fp["item_index"]: fp for fp in fingerprints}
    cache: Dict[int, List[Tuple[List[Any], np.ndarray]]] = {}
    for item_idx, fp in fp_by_idx.items():
        orb_variants = fp.get("orb_variants")
        if not orb_variants:
            orb_kps = fp.get("orb_kps")
            orb_desc = fp.get("orb_desc")
            if orb_desc is not None and orb_kps:
                orb_variants = [(orb_kps, orb_desc)]
        if not orb_variants:
            continue
        scored: List[Tuple[int, int]] = []
        scored_size: List[Tuple[float, int]] = []
        details_by_post: Dict[int, Optional[str]] = {}
        score_by_post: Dict[int, int] = {}
        for row in candidates_pool:
            _d, _p, _w, details, score = _hash_distance_details(fp, row)
            if score is None:
                continue
            post_id = int(row["post_id"])
            details_by_post[post_id] = details
            score_by_post[post_id] = score
            scored.append((score, post_id))
            size_score = _size_similarity_score(fp, row)
            if size_score is not None:
                scored_size.append((size_score, post_id))
        if not scored:
            continue
        scored.sort(key=lambda item: (item[0], item[1]))
        scored_size.sort(key=lambda item: (item[0], item[1]))
        selected: List[Tuple[int, int, Optional[str]]] = []
        seen: set[int] = set()
        for score, post_id in scored[:DUPLICATE_ORB_TOPK]:
            if post_id in seen:
                continue
            seen.add(post_id)
            selected.append((score, post_id, details_by_post.get(post_id)))
        for _s, post_id in scored_size[:DUPLICATE_ORB_TOPK_SIZE]:
            if post_id in seen:
                continue
            seen.add(post_id)
            score = score_by_post.get(post_id)
            if score is None:
                continue
            selected.append((score, post_id, details_by_post.get(post_id)))
        for score, post_id, details in selected:
            cand_feats = await _get_orb_features_for_post(post_id, cache)
            if not cand_feats:
                continue
            best = None
            best_key = None
            for cand_kps, cand_desc in cand_feats:
                for var_kps, var_desc in orb_variants:
                    result = _orb_match_metrics(var_kps, var_desc, cand_kps, cand_desc)
                    if result is None:
                        continue
                    good, good_ratio, inliers, inlier_ratio = result
                    if good < DUPLICATE_ORB_MIN_GOOD:
                        continue
                    if good < DUPLICATE_ORB_MIN_MATCHES and good_ratio < DUPLICATE_ORB_MIN_RATIO:
                        continue
                    if DUPLICATE_ORB_RANSAC_MIN_INLIERS > 0:
                        if good < 4:
                            continue
                        strict_ok = (
                            inliers >= DUPLICATE_ORB_RANSAC_MIN_INLIERS
                            and inlier_ratio >= DUPLICATE_ORB_RANSAC_MIN_RATIO
                        )
                        loose_ok = (
                            inliers >= DUPLICATE_ORB_RANSAC_MIN_INLIERS_LOOSE
                            and inlier_ratio >= DUPLICATE_ORB_RANSAC_MIN_RATIO_LOOSE
                            and good_ratio >= DUPLICATE_ORB_MIN_RATIO_LOOSE
                        )
                        if not (strict_ok or loose_ok):
                            continue
                        key = (inliers, inlier_ratio, good, good_ratio)
                    else:
                        key = (good, good_ratio)
                    if best_key is None or key > best_key:
                        best_key = key
                        best = (good, good_ratio, inliers, inlier_ratio)
            if best is None:
                continue
            best_good, best_ratio, best_inliers, best_inlier_ratio = best
            orb_parts = [f"orb={best_good}", f"r={best_ratio:.2f}"]
            if DUPLICATE_ORB_RANSAC_MIN_INLIERS > 0:
                orb_parts.append(f"inl={best_inliers}")
                orb_parts.append(f"ir={best_inlier_ratio:.2f}")
            orb_part = ",".join(orb_parts)
            updated = False
            for m in matches:
                if int(m.get("item_index", -1)) != item_idx:
                    continue
                if int(m.get("post_id", -1)) != post_id:
                    continue
                existing = m.get("details")
                if existing:
                    if "orb=" not in existing:
                        m["details"] = f"{existing},{orb_part}"
                else:
                    m["details"] = orb_part
                updated = True
                break
            if updated:
                continue
            new_details = details
            if new_details:
                new_details = f"{new_details},{orb_part}"
            else:
                new_details = orb_part
            matches.append(
                {
                    "item_index": item_idx,
                    "match_type": "orb_fallback",
                    "post_id": post_id,
                    "distance": score,
                    "details": new_details,
                }
            )
    return matches

async def compute_duplicate_result_deep_images(
    content: DraftContent,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    fingerprints = await compute_image_fingerprints(content)
    matches = await detect_duplicate_images_deep(fingerprints)
    if fingerprints:
        try:
            matches = await annotate_matches_with_orb(fingerprints, matches)
        except Exception as e:
            logger.warning("ORB verification failed: %s", e)
    matches = filter_duplicate_matches(matches)
    return fingerprints, matches

async def compute_duplicate_result_deep_videos(
    content: DraftContent,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    fingerprints = await compute_video_fingerprints(content)
    matches = await detect_duplicate_videos_deep(fingerprints)
    return fingerprints, matches

async def compute_duplicate_result_deep(
    content: DraftContent,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    image_fps: List[Dict[str, Any]] = []
    video_fps: List[Dict[str, Any]] = []
    matches: List[Dict[str, Any]] = []
    if content.kind == "album" and len(content.items) > 1:
        video_fps = await compute_album_video_fingerprints(content)
        if video_fps:
            matches = await detect_duplicate_videos_deep(video_fps)
        return image_fps, video_fps, matches
    if content_has_images(content):
        image_fps, image_matches = await compute_duplicate_result_deep_images(content)
        matches.extend(image_matches)
    if content_has_videos(content):
        video_fps, video_matches = await compute_duplicate_result_deep_videos(content)
        matches.extend(video_matches)
    return image_fps, video_fps, matches

def _parse_hash_distances(details: Optional[str]) -> Dict[str, int]:
    if not details:
        return {}
    found = re.findall(r"\b([dpw])=(\d+)", details)
    return {key: int(val) for key, val in found}

def _is_strong_hash(distances: Dict[str, int]) -> bool:
    if not distances:
        return False
    fast = {"d": DUPLICATE_DHASH_THRESHOLD, "p": DUPLICATE_PHASH_THRESHOLD, "w": DUPLICATE_WHASH_THRESHOLD}
    hits_fast = 0
    for key, dist in distances.items():
        if dist <= fast.get(key, 0):
            hits_fast += 1
    if hits_fast >= 2:
        return True
    min_dist = min(distances.values()) if distances else None
    if min_dist is not None and min_dist <= DUPLICATE_SINGLE_HASH_THRESHOLD:
        return True
    return False

def filter_duplicate_matches(matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for match in matches:
        match_type = match.get("match_type")
        if match_type == "unique_id":
            filtered.append(match)
            continue
        if match_type and str(match_type).startswith("video"):
            filtered.append(match)
            continue
        details = match.get("details") or ""
        if "orb=" in details or match_type == "orb_fallback":
            filtered.append(match)
            continue
        if match_type == "hash_fast":
            filtered.append(match)
            continue
        distances = _parse_hash_distances(details)
        if _is_strong_hash(distances):
            filtered.append(match)
    return filtered

def format_duplicate_info(
    matches: Optional[List[Dict[str, Any]]],
    *,
    pending: bool = False,
    always_show: bool = False,
) -> str:
    if pending:
        return "Повторки: ищу в базе данных..."
    if matches is None:
        return ""
    if not matches:
        return "Повторки: совпадений с опубликованными не найдено" if always_show else ""

    unique: Dict[int, Dict[str, Any]] = {}
    for m in matches:
        post_id = int(m["post_id"])
        dist = int(m.get("distance", 0))
        prev = unique.get(post_id)
        if not prev or dist < int(prev.get("distance", 0)):
            unique[post_id] = {
                "post_id": post_id,
                "distance": dist,
                "match_type": m.get("match_type"),
                "details": m.get("details"),
            }

    ordered = sorted(unique.values(), key=lambda item: (int(item.get("distance", 0)), int(item.get("post_id", 0))))
    parts = []
    for item in ordered:
        post_id = item.get("post_id")
        details = item.get("details")
        if details:
            parts.append(f"#id{post_id} ({details})")
        else:
            match_type = item.get("match_type")
            if match_type == "unique_id":
                parts.append(f"#id{post_id} (точная копия)")
            else:
                parts.append(f"#id{post_id}")
    return "Повторки: возможный повтор (опубликованные):\n" + ",\n".join(parts)

async def finalize_duplicate_check_for_post(
    post_id: int,
    deep_task: asyncio.Task[Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]],
):
    try:
        image_fps, video_fps, matches = await deep_task
        if image_fps or video_fps:
            try:
                if image_fps:
                    await db.add_image_fingerprints(post_id, image_fps)
                if video_fps:
                    await db.add_video_fingerprints(post_id, video_fps)
            except Exception as e:
                logger.warning("Failed to save fingerprints for post %s: %s", post_id, e)
            dup_info = format_duplicate_info(matches, always_show=True)
        else:
            dup_info = "Повторки: не удалось проверить"
        try:
            await db.set_post_duplicate_info(post_id, dup_info)
        except Exception as e:
            logger.warning("Failed to store duplicate info for post %s: %s", post_id, e)
    except asyncio.CancelledError:
        return
    except Exception as e:
        logger.warning("Deep duplicate check failed for post %s: %s", post_id, e)
        with contextlib.suppress(Exception):
            await db.set_post_duplicate_info(post_id, "Повторки: ошибка проверки")

    with contextlib.suppress(Exception):
        await update_admin_view(post_id)

async def send_content_copy(
    dest_chat: int,
    content: DraftContent,
    *,
    caption: str,
    reply_markup: Optional[InlineKeyboardMarkup] = None,
    force_buttons_message: bool = False,
) -> Tuple[int, List[int]]:
    """Send stored content to a chat, returning first message id and list of all ids."""
    message_ids: List[int] = []
    if content.kind == "album":
        builder = MediaGroupBuilder()
        first = True
        for item in content.items:
            item_caption = None  # альбомы отправляем одним сообвещнием а кнопки другим
            first = False
            if item["type"] == "photo":
                builder.add_photo(media=item["file_id"], caption=item_caption)
            elif item["type"] == "video":
                builder.add_video(media=item["file_id"], caption=item_caption)
            elif item["type"] == "document":
                builder.add_document(media=item["file_id"], caption=item_caption)
            elif item["type"] == "audio":
                builder.add_audio(media=item["file_id"], caption=item_caption)
        messages = await bot.send_media_group(dest_chat, media=builder.build())
        message_ids.extend(m.message_id for m in messages)
        if reply_markup or force_buttons_message:
            btn_msg = await bot.send_message(dest_chat, caption, reply_markup=reply_markup)
            message_ids.append(btn_msg.message_id)
            return btn_msg.message_id, message_ids
        return messages[0].message_id, message_ids

    if content.kind == "photo":
        msg = await bot.send_photo(dest_chat, content.items[0]["file_id"], caption=caption, reply_markup=reply_markup)
    elif content.kind == "video":
        msg = await bot.send_video(dest_chat, content.items[0]["file_id"], caption=caption, reply_markup=reply_markup)
    elif content.kind == "animation":
        msg = await bot.send_animation(dest_chat, content.items[0]["file_id"], caption=caption, reply_markup=reply_markup)
    elif content.kind == "document":
        msg = await bot.send_document(dest_chat, content.items[0]["file_id"], caption=caption, reply_markup=reply_markup)
    elif content.kind == "audio":
        msg = await bot.send_audio(dest_chat, content.items[0]["file_id"], caption=caption, reply_markup=reply_markup)
    elif content.kind == "voice":
        msg = await bot.send_voice(dest_chat, content.items[0]["file_id"], caption=caption, reply_markup=reply_markup)
    elif content.kind == "video_note":
        msg = await bot.send_video_note(dest_chat, content.items[0]["file_id"], reply_markup=reply_markup)
    elif content.kind == "text":
        msg = await bot.send_message(dest_chat, caption, reply_markup=reply_markup)
    else:
        raise ValueError("Unsupported content kind")
    message_ids.append(msg.message_id)
    return msg.message_id, message_ids

async def schedule_next_slot(now: datetime) -> datetime:
    """Find closest free slot according to Chronos config (legacy static)."""
    cfg = await get_chronos_config()
    if cfg.instant_publish:
        return now.astimezone(TZ)
    booked = set(await db.list_scheduled_times())
    for candidate in slot_iterator(now, cfg):
        if candidate not in booked:
            return candidate
    return now.astimezone(TZ)

def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

def _daily_capacity(cfg: ChronosConfig) -> int:
    ts = cfg.start_hour * 60
    te = cfg.end_hour * 60
    step = max(1, cfg.step_minutes)
    if te >= ts:
        return int(math.floor((te - ts) / step)) + 1
    return int(math.floor(((te + 1440) - ts) / step)) + 1

async def run_dynamic_planner(now: datetime, target_post_id: Optional[int] = None) -> Optional[datetime]:
    cfg = await get_chronos_config()
    if cfg.instant_publish:
        return now
    queue = await db.get_scheduled_posts()
    if not queue:
        return None
    Q = len(queue)
    cap = max(1, _daily_capacity(cfg))
    n_hardmax = cap
    n_softmax = min(10, cap)


    oldest_ts = None
    pending_by_author: Dict[int, int] = {}
    for row in queue:
        pending_by_author[row["user_id"]] = pending_by_author.get(row["user_id"], 0) + 1
        ts_raw = row["approved_at"] or row["created_at"]
        if ts_raw:
            ts = datetime.fromisoformat(ts_raw)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=TZ)
            if oldest_ts is None or ts < oldest_ts:
                oldest_ts = ts
    if oldest_ts is None:
        oldest_ts = now
    W_hours = max(0.0, (now - oldest_ts).total_seconds() / 3600.0)
    max_pending_author = max(pending_by_author.values()) if pending_by_author else 1
    W_eff = W_hours / (1 + math.log1p(max_pending_author))

    


    hist = await db.approvals_history(14)
    today = now.astimezone(TZ).date()
    counts = []
    for delta in range(14, 0, -1):
        d = today - timedelta(days=delta - 1)
        counts.append(hist.get(d, 0))
    alpha_long = 1 / 14
    alpha_short = 1 / 3
    lambda_long = _ewma(counts, alpha_long) if counts else 0.0
    short_window = counts[-3:] if counts else []
    lambda_short = _ewma(short_window, alpha_short) if short_window else lambda_long
    lambda_pred = 0.7 * lambda_short + 0.3 * lambda_long

    n_soft = 1 + (n_softmax - 1) * _sigmoid((Q - n_softmax) / (0.25 * n_softmax if n_softmax else 1))
    boost = _sigmoid((W_eff - 22) / 2.5)
    n_day_target = n_soft + (n_hardmax - n_softmax) * boost
    n_day_target = max(n_day_target, lambda_pred)
    if Q == 0:
        n_day = 0
    else:
        last_plan_raw = await db.get_setting("chronos_last_plan", None)
        try:
            last_plan = float(last_plan_raw) if last_plan_raw is not None else n_day_target
        except Exception:
            last_plan = n_day_target
        beta = 0.3
        n_plan_smooth = (1 - beta) * last_plan + beta * n_day_target
        n_day = max(1, min(Q, n_hardmax, round(n_plan_smooth)))
        await db.set_setting("chronos_last_plan", str(n_day))


    slots_today_remaining = 0
    today_date = now.astimezone(TZ).date()
    for slot in slot_iterator(now, cfg):
        if slot.date() != today_date:
            break
        slots_today_remaining += 1
    r = slots_today_remaining / cap if cap else 0
    g_today = _sigmoid((r - 0.35) / 0.10) if cap else 0
    n_today_base = min(slots_today_remaining, max(0, round(n_day * g_today)))
    # мягко тяну очередь в первый день сигмоидой
    pull = _sigmoid((12 - Q) / 3)
    first_day_cap = min(Q, slots_today_remaining, cap)
    n_today = round(n_today_base * (1 - pull) + first_day_cap * pull)
    if n_today == 0 and Q > 0 and slots_today_remaining > 0:
        n_today = 1
    daily_target_next = max(1, min(n_hardmax, round(n_day_target))) if Q > 0 else 0

    H_days = 0.25 + 6.75 * _sigmoid((Q - 20) / 10)
    max_days = max(1, math.ceil(H_days))

    
    recent_authors = await db.last_published_authors(limit=10)
    recent_counts: Dict[int, int] = {}
    for a in recent_authors:
        recent_counts[a] = recent_counts.get(a, 0) + 1
    last_published_map = await db.last_published_map()
    last_author = recent_authors[0] if recent_authors else None


    queue_sorted = sorted(queue, key=lambda r: (r["approved_at"] or r["created_at"], r["id"]))
    remaining: List[aiosqlite.Row] = queue_sorted.copy()
    assigned: Dict[int, datetime] = {}
    day_usage: Dict[datetime.date, int] = {}

    def allowed_for_date(d: datetime.date) -> int:
        if d == today_date:
            return min(cap, n_today)
        return min(cap, daily_target_next)

    # собрать слоты по дням в горизонте
    slots_by_day: Dict[datetime.date, List[datetime]] = {}
    max_day_date = now.date() + timedelta(days=max_days + 2)
    for slot in slot_iterator(now, cfg):
        if slot.date() > max_day_date:
            break
        if slot < now and slot.date() == today_date:
            continue
        day = slot.date()
        slots_by_day.setdefault(day, [])
        if len(slots_by_day[day]) < cap:
            slots_by_day[day].append(slot)
        if len(slots_by_day) >= max_days + 2 and all(len(v) >= cap for v in slots_by_day.values() if v):
            break


    selected_slots: List[datetime] = []
    for day in sorted(slots_by_day.keys()):
        day_slots = slots_by_day[day]
        if not day_slots:
            continue
        day_usage.setdefault(day, 0)
        allowed = allowed_for_date(day)
        if allowed <= 0:
            continue
        allowed = min(allowed, len(day_slots))
        step = len(day_slots) / allowed
        indexes = sorted({int(math.floor(i * step)) for i in range(allowed)})
        for idx in indexes:
            if idx < len(day_slots):
                selected_slots.append(day_slots[idx])

    selected_slots.sort()
    for slot in selected_slots:
        if not remaining:
            break
        day = slot.date()
        day_usage.setdefault(day, 0)
        if day_usage[day] >= allowed_for_date(day):
            continue

        window_size = min(len(remaining), max(30, 3 * max(1, daily_target_next)))
        window = remaining[:window_size]
        authors_in_window = {r["user_id"] for r in window}
        forbid_last = last_author in authors_in_window and len(authors_in_window) > 1
        candidates = [r for r in window if not (forbid_last and r["user_id"] == last_author)]
        if not candidates:
            candidates = window
 
        best_author = None
        best_prio = -1e9
        now_ts = slot
        for r in candidates:
            uid = r["user_id"]
            pending_u = pending_by_author.get(uid, 1)
            last_pub = last_published_map.get(uid)
            t_u = (now_ts - last_pub).total_seconds() / 3600 if last_pub else 1e3
            recent_u = recent_counts.get(uid, 0)
            prio = math.log1p(t_u) / (1 + math.log1p(pending_u)) - 0.3 * recent_u
            if prio > best_prio:
                best_prio = prio
                best_author = uid
        chosen_idx = None
        for idx, r in enumerate(remaining):
            if r["user_id"] == best_author:
                chosen_idx = idx
                break
        if chosen_idx is None:
            chosen_idx = 0
        chosen = remaining.pop(chosen_idx)
        assigned[chosen["id"]] = slot
        pending_by_author[chosen["user_id"]] = max(0, pending_by_author.get(chosen["user_id"], 1) - 1)
        recent_counts[chosen["user_id"]] = recent_counts.get(chosen["user_id"], 0) + 1
        recent_authors.append(chosen["user_id"])
        if len(recent_authors) > 10:
            drop = recent_authors.pop(0)
            cnt = recent_counts.get(drop, 0)
            if cnt <= 1:
                recent_counts.pop(drop, None)
            else:
                recent_counts[drop] = cnt - 1
        last_author = chosen["user_id"]
        last_published_map[chosen["user_id"]] = slot
        day_usage[day] += 1

    # обновить бд
    scheduled_for_target = None
    for pid, ts in assigned.items():
        await db.set_post_status(pid, "scheduled", scheduled_at=ts)
        if target_post_id is not None and pid == target_post_id:
            scheduled_for_target = ts
    for pid in assigned.keys():
        await update_admin_view(pid)
    return scheduled_for_target

async def schedule_post(post_id: int) -> datetime:
    now = datetime.now(TZ)
    post = await db.get_post(post_id)
    approved_now = False
    if post and not post["approved_at"]:
        approved_now = True
        await db.increment_approval(now.date())
    cfg = await get_chronos_config()
    mode = await get_chronos_mode()
    if cfg.instant_publish:
        await db.set_post_status(post_id, "scheduled", scheduled_at=now, approved_at=now if approved_now else None)
        return now
    await db.set_post_status(post_id, "scheduled", approved_at=now if approved_now else None)
    if mode == "dynamic":
        scheduled = await run_dynamic_planner(now, target_post_id=post_id)
        if scheduled is None:
            scheduled = now
    else:
        scheduled = await schedule_next_slot(now)
        await db.set_post_status(post_id, "scheduled", scheduled_at=scheduled)
    return scheduled

async def publish_scheduled_post(post_row):
    content = DraftContent(**json.loads(post_row["media_json"]))
    user_row = await db.get_user_by_id(post_row["user_id"])
    if not user_row:
        return
    hashtag = user_row["hashtag"] or ""
    caption_base = post_row["caption"] or ""
    parts = []
    if caption_base:
        parts.append(escape(caption_base))
    if hashtag:
        parts.append(f"#{escape(hashtag)}")
    parts.append('<a href="https://t.me/goldencumbot">Goldencum. Скинуть мем</a>')
    final_caption = truncate_caption("\n\n".join(parts))
    channel_message_id = None
    try:
        message_id, _ = await send_content_copy(
            CHANNEL_ID,
            content,
            caption=final_caption,
        )
        channel_message_id = message_id
        await db.set_post_status(
            post_row["id"],
            "published",
            channel_message_id=channel_message_id,
            notified_status="published",
            published_at=datetime.now(TZ),
        )
    except Exception as e:
        logger.error("Failed to publish post %s: %s", post_row["id"], e)

async def scheduler_loop():
    while True:
        try:
            now = datetime.now(TZ)
            if await is_bot_paused():
                await asyncio.sleep(5)
                continue
            due = await db.due_posts(now)
            for post_row in due:
                await publish_scheduled_post(post_row)
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            break
        except Exception as e: 
            logger.exception("Scheduler error: %s", e)
            await asyncio.sleep(5)

@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return
    await db.upsert_user(message.from_user.id, message.from_user.username)
    user = await db.get_user_by_tg(message.from_user.id)
    hashtag = user["hashtag"] if user else None
    if not hashtag:
        await state.set_state(HashtagFlow.waiting_hashtag)
        await message.answer(
            "Давайте зададим ваш персональный хэштег.\n"
            "Разрешены буквы (а-я, A-Z) и цифры, без пробелов и спецсимволов, до 28 символов.\n"
            "Введите хэштег без '#'.",
            reply_markup=ReplyKeyboardMarkup(keyboard=[], resize_keyboard=True),
        )
        return
    await message.answer("Добро пожаловать в предложку канала Недолёт Мыслей Икара. "
        "Бот управляется с помощью кнопок, которые находятся ниже. "
        "Советуем также прочитать раздел 'Помощь'", reply_markup=MAIN_MENU)

@dp.message(Command(commands=["ban_hashtag", "unban_hashtag"]))
async def admin_ban_commands(message: Message, command: CommandObject):
    if not await is_super_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Укажите хэштег: /ban_hashtag tag")
        return
    hashtag = parts[1].strip().lstrip("#")
    user = await db.get_user_by_hashtag(hashtag)
    if not user:
        await message.reply("Пользователь с таким хэштегом не найден.")
        return
    banned = command.command == "ban_hashtag"
    await db.mark_banned(user["id"], banned)
    await message.reply(f"Хэштег #{hashtag} {'забанен' if banned else 'разбанен'}.")

@dp.message(Command(commands=["broadcast"]))
async def broadcast(message: Message, command: CommandObject):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        await message.answer("Команда доступна только в личных сообщениях.")
        return
    payload = (command.args or "").strip()
    use_reply = message.reply_to_message is not None
    if not payload and not use_reply:
        await message.answer("Пришлите /broadcast <текст> или ответьте /broadcast на сообщение для рассылки.")
        return
    recipients = await db.list_user_chat_ids()
    if not recipients:
        await message.answer("Нет пользователей для рассылки.")
        return
    sent = 0
    for chat_id in recipients:
        try:
            if use_reply and message.reply_to_message:
                await bot.copy_message(
                    chat_id,
                    from_chat_id=message.chat.id,
                    message_id=message.reply_to_message.message_id,
                )
            else:
                await bot.send_message(chat_id, payload)
            sent += 1
        except Exception as e:
            logger.warning("Broadcast to %s failed: %s", chat_id, e)
        await asyncio.sleep(0.05)
    await message.answer(f"Рассылка завершена: {sent}/{len(recipients)} доставлено.")

@dp.message(Command(commands=["backfilldups"]))
async def backfill_dups(message: Message, command: CommandObject):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    parts = (command.args or "").split()
    if not parts:
        await message.answer("Формат: /backfilldups <кол-во постов> [force]")
        return
    try:
        requested = int(parts[0])
    except ValueError:
        await message.answer("Кол-во постов должно быть числом.")
        return
    if requested <= 0:
        await message.answer("Кол-во постов должно быть больше 0.")
        return
    force = any(p.lower() in {"force", "f", "-f", "rebuild"} for p in parts[1:])

    limit = min(requested, DUPLICATE_BACKFILL_MAX_POSTS)
    if limit != requested:
        await message.answer(f"Ограничиваю бэкфилл до {limit} постов.")

    if force:
        rows = await db.list_recent_posts(limit)
    else:
        rows_img = await db.list_recent_posts_without_fingerprints(limit)
        rows_vid = await db.list_recent_posts_without_video_fingerprints(limit)
        rows_map = {row["id"]: row for row in list(rows_img) + list(rows_vid)}
        rows = sorted(rows_map.values(), key=lambda row: row["id"], reverse=True)[:limit]
    if not rows:
        await message.answer("Нет постов без отпечатков для бэкфилла.")
        return

    total = len(rows)
    rows = list(reversed(rows))
    mode_label = "форс" if force else "обычный"
    status_msg = await message.answer(f"Бэкфилл дублей ({mode_label}): найдено {total} постов, начинаю...")

    processed = 0
    skipped = 0
    no_media = 0
    errors = 0
    last_update = time.monotonic()

    for idx, row in enumerate(rows, start=1):
        media_json = row["media_json"] or ""
        if not media_json:
            skipped += 1
            continue
        try:
            raw = json.loads(media_json)
            if not isinstance(raw, dict):
                skipped += 1
                continue
            kind = raw.get("kind")
            items = raw.get("items")
            if not kind or not isinstance(items, list):
                skipped += 1
                continue
            content = DraftContent(kind=kind, items=items, caption=raw.get("caption") or "")
        except Exception:
            errors += 1
            continue

        if not content_has_images(content) and not content_has_videos(content):
            no_media += 1
            continue

        try:
            if force:
                await db.delete_image_fingerprints(row["id"])
                await db.delete_video_fingerprints(row["id"])
            image_fps, video_fps, matches = await compute_duplicate_result_deep(content)
            if not image_fps and not video_fps:
                no_media += 1
                continue
            dup_info = format_duplicate_info(matches, always_show=True)
            await db.set_post_duplicate_info(row["id"], dup_info)
            if image_fps:
                await db.add_image_fingerprints(row["id"], image_fps)
            if video_fps:
                await db.add_video_fingerprints(row["id"], video_fps)
            processed += 1
        except Exception as e:
            errors += 1
            logger.warning("Backfill failed for post %s: %s", row["id"], e)

        if time.monotonic() - last_update >= 2:
            await status_msg.edit_text(
                f"Бэкфилл дублей ({mode_label}): {idx}/{total}. Готово {processed}, без медиа {no_media}, пропущено {skipped}, ошибки {errors}."
            )
            last_update = time.monotonic()

    await status_msg.edit_text(
        f"Бэкфилл завершён ({mode_label}): всего {total}, готово {processed}, без медиа {no_media}, пропущено {skipped}, ошибки {errors}."
    )

@dp.message(Command(commands=["backfillchannel"]))
async def backfill_channel(message: Message, command: CommandObject):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    parts = (command.args or "").split()
    if not parts:
        await message.answer("Формат: /backfillchannel <кол-во постов> [force]")
        return
    try:
        requested = int(parts[0])
    except ValueError:
        await message.answer("Кол-во постов должно быть числом.")
        return
    if requested <= 0:
        await message.answer("Кол-во постов должно быть больше 0.")
        return
    force = any(p.lower() in {"force", "f", "-f", "rebuild"} for p in parts[1:])

    if TelegramClient is None or StringSession is None:
        await message.answer("Telethon не установлен. Установи зависимость: pip install telethon")
        return
    if not TELETHON_API_ID or not TELETHON_API_HASH:
        await message.answer("Нужно задать TELETHON_API_ID и TELETHON_API_HASH в .env.")
        return
    if SYSTEM_USER_TG_ID <= 0:
        await message.answer("Нужно задать SYSTEM_USER_TG_ID (id служебного пользователя) в .env.")
        return
    try:
        api_id = int(TELETHON_API_ID)
    except Exception:
        await message.answer("TELETHON_API_ID должен быть числом.")
        return

    session: Any
    if TELETHON_SESSION_STRING:
        session = StringSession(TELETHON_SESSION_STRING)
    else:
        session_name = TELETHON_SESSION or "channel_backfill"
        session_file = session_name if session_name.endswith(".session") else f"{session_name}.session"
        if not os.path.exists(session_file):
            await message.answer(
                "Нужна авторизованная Telethon-сессия. "
                "Укажи TELETHON_SESSION_STRING или положи файл сессии "
                f"{session_file} рядом с ботом."
            )
            return
        session = session_name

    limit = min(requested, DUPLICATE_BACKFILL_MAX_POSTS)
    if limit != requested:
        await message.answer(f"Ограничиваю бэкфилл до {limit} постов.")

    system_user = await db.get_or_create_system_user()
    if not system_user:
        await message.answer("Не удалось создать служебного пользователя.")
        return

    known_ids = set(await db.list_channel_message_ids())
    mode_label = "форс" if force else "обычный"
    status_msg = await message.answer("Бэкфилл канала: подключаюсь...")

    items: List[Any] = []
    created = 0
    updated = 0
    skipped = 0
    no_media = 0
    errors = 0
    total = 0
    last_update = time.monotonic()

    try:
        async with TelegramClient(session, api_id, TELETHON_API_HASH) as client:
            if not await client.is_user_authorized():
                await status_msg.edit_text(
                    "Telethon-сессия не авторизована. Укажи TELETHON_SESSION_STRING."
                )
                return

            def _has_media(msg: Any) -> bool:
                if getattr(msg, "photo", None) or getattr(msg, "video", None) or getattr(msg, "gif", None):
                    return True
                doc = getattr(msg, "document", None)
                if doc is None:
                    return False
                mime = (getattr(doc, "mime_type", None) or "").lower()
                return mime.startswith("image/") or mime.startswith("video/")

            current_group_id = None
            current_group: List[Any] = []
            count_posts = 0
            reached_limit = False
            async for msg in client.iter_messages(CHANNEL_ID):
                if msg is None or not getattr(msg, "id", None):
                    continue
                gid = getattr(msg, "grouped_id", None)
                if gid:
                    if current_group_id is None:
                        current_group_id = gid
                        current_group = [msg]
                    elif gid == current_group_id:
                        current_group.append(msg)
                    else:
                        items.append(sorted(current_group, key=lambda m: m.id))
                        count_posts += 1
                        if count_posts >= limit:
                            reached_limit = True
                            break
                        current_group_id = gid
                        current_group = [msg]
                    continue

                if current_group_id is not None:
                    items.append(sorted(current_group, key=lambda m: m.id))
                    count_posts += 1
                    if count_posts >= limit:
                        reached_limit = True
                        break
                    current_group_id = None
                    current_group = []

                if not _has_media(msg):
                    continue
                items.append(msg)
                count_posts += 1
                if count_posts >= limit:
                    reached_limit = True
                    break

            if not reached_limit and current_group_id is not None:
                items.append(sorted(current_group, key=lambda m: m.id))

            if not items:
                await status_msg.edit_text("Бэкфилл канала: подходящие посты не найдены.")
                return

            total = len(items)
            items = list(reversed(items))
            await status_msg.edit_text(f"Бэкфилл канала ({mode_label}): найдено {total} постов, начинаю...")

            for idx, item in enumerate(items, start=1):
                try:
                    if isinstance(item, list):
                        messages = item
                        message_ids = [m.id for m in messages if getattr(m, "id", None)]
                        if not message_ids:
                            skipped += 1
                            continue
                        channel_message_id = min(message_ids)
                        if not force and channel_message_id in known_ids:
                            skipped += 1
                            continue
                        existing = None
                        if force:
                            existing = await db.get_post_by_channel_message_id(channel_message_id)
                            if existing and existing["user_id"] != system_user["id"]:
                                skipped += 1
                                continue
                        caption = ""
                        for msg in messages:
                            text = (getattr(msg, "message", None) or "").strip()
                            if text:
                                caption = text
                                break
                        published_at = messages[0].date if getattr(messages[0], "date", None) else datetime.now(TZ)
                        if published_at.tzinfo is None:
                            published_at = published_at.replace(tzinfo=timezone.utc)
                        media_type = "album"
                        image_fps: List[Dict[str, Any]] = []
                        video_fps = await _telethon_album_fingerprints(client, messages)
                        if not video_fps:
                            no_media += 1
                            continue
                    else:
                        msg = item
                        channel_message_id = getattr(msg, "id", None)
                        if channel_message_id is None:
                            skipped += 1
                            continue
                        if not force and channel_message_id in known_ids:
                            skipped += 1
                            continue
                        existing = None
                        if force:
                            existing = await db.get_post_by_channel_message_id(channel_message_id)
                            if existing and existing["user_id"] != system_user["id"]:
                                skipped += 1
                                continue
                        caption = (getattr(msg, "message", None) or "").strip()
                        published_at = msg.date if getattr(msg, "date", None) else datetime.now(TZ)
                        if published_at.tzinfo is None:
                            published_at = published_at.replace(tzinfo=timezone.utc)
                        media_type, image_fps, video_fps = await _telethon_message_fingerprints(client, msg)
                        if not image_fps and not video_fps:
                            no_media += 1
                            continue
                        if not media_type:
                            media_type = "document" if image_fps else "video"

                    media_json = json.dumps({"kind": media_type, "items": [], "caption": caption})

                    if existing:
                        await db.delete_image_fingerprints(existing["id"])
                        await db.delete_video_fingerprints(existing["id"])
                        await db.db.execute(
                            "UPDATE posts SET media_type=?, caption=?, media_json=? WHERE id=?",
                            (media_type, caption, media_json, existing["id"]),
                        )
                        await db.db.commit()
                        post_id = existing["id"]
                        updated += 1
                    else:
                        post_id = await db.create_post(
                            system_user["id"],
                            media_type=media_type,
                            caption=caption,
                            media_json=media_json,
                            status="published",
                        )
                        await db.set_post_status(
                            post_id,
                            "published",
                            channel_message_id=channel_message_id,
                            notified_status="published",
                            published_at=published_at,
                        )
                        known_ids.add(channel_message_id)
                        created += 1

                    if image_fps:
                        await db.add_image_fingerprints(post_id, image_fps)
                    if video_fps:
                        await db.add_video_fingerprints(post_id, video_fps)
                except Exception as e:
                    errors += 1
                    logger.warning("Channel backfill failed for item %s: %s", idx, e)

                if time.monotonic() - last_update >= 2:
                    await status_msg.edit_text(
                        f"Бэкфилл канала ({mode_label}): {idx}/{total}. Создано {created}, обновлено {updated}, "
                        f"без медиа {no_media}, пропущено {skipped}, ошибки {errors}."
                    )
                    last_update = time.monotonic()
    except Exception as e:
        await status_msg.edit_text(f"Бэкфилл канала: ошибка работы Telethon ({e}).")
        return

    await status_msg.edit_text(
        f"Бэкфилл канала ({mode_label}) завершён: всего {total}, создано {created}, обновлено {updated}, "
        f"без медиа {no_media}, пропущено {skipped}, ошибки {errors}."
    )

@dp.message(Command(commands=["pausebot"]))
async def pause_bot(message: Message):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    await db.set_setting("bot_paused", "1")
    await message.answer("Бот приостановлен: обработка сообщений и публикации остановлены.")

@dp.message(Command(commands=["resumebot"]))
async def resume_bot(message: Message):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    await db.set_setting("bot_paused", "0")
    await message.answer("Бот возобновлён: работа продолжена.")

@dp.message(Command(commands=["superadd"]))
async def super_add(message: Message, command: CommandObject):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    parts = (command.args or "").split()
    if not parts:
        await message.answer("Формат: /superadd user_id")
        return
    try:
        uid = int(parts[0])
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return
    await add_super_admin(uid)
    await message.answer(f"Добавлен суперадмин: {uid}")

@dp.message(Command(commands=["superdel"]))
async def super_del(message: Message, command: CommandObject):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    parts = (command.args or "").split()
    if not parts:
        await message.answer("Формат: /superdel user_id")
        return
    try:
        uid = int(parts[0])
    except ValueError:
        await message.answer("user_id должен быть числом.")
        return
    await remove_super_admin(uid)
    await message.answer(f"Удалён из суперадминов: {uid}")

@dp.message(Command(commands=["superlist"]))
async def super_list(message: Message):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    ids = sorted(await get_super_admin_ids())
    await message.answer("Список суперадминов:\n" + "\n".join(str(i) for i in ids))

@dp.message(Command(commands=["botnow"]))
async def bot_now(message: Message):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    now = datetime.now(TZ)
    await message.answer(now.strftime("Текущее время бота (TZ %z): %d/%m/%Y %H:%M:%S"))

@dp.message(Command(commands=["cancelpost"]))
async def cancel_post(message: Message, command: CommandObject):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    parts = (command.args or "").split()
    if not parts:
        await message.answer("Формат: /cancelpost post_id")
        return
    try:
        post_id = int(parts[0])
    except ValueError:
        await message.answer("post_id должен быть числом.")
        return
    post = await db.get_post(post_id)
    if not post:
        await message.answer("Пост не найден.")
        return
    if post["status"] != "scheduled":
        await message.answer("Этот пост не в отложке.")
        return
    await db.db.execute("UPDATE posts SET status='rejected', scheduled_at=NULL WHERE id=?", (post_id,))
    await db.db.commit()
    await update_admin_view(post_id)
    await message.answer(f"Пост #id{post_id} снят с расписания и отменён.")
    cfg = await get_chronos_config()
    if not cfg.instant_publish and (await get_chronos_mode()) == "dynamic":
        await run_dynamic_planner(datetime.now(TZ))

@dp.message(Command(commands=["instanton"]))
async def instant_on(message: Message):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    cfg = await get_chronos_config()
    new_cfg = ChronosConfig(
        start_hour=cfg.start_hour, end_hour=cfg.end_hour, step_minutes=cfg.step_minutes, instant_publish=True
    )
    await set_chronos_config(new_cfg)
    await message.answer("Instant-режим включён. Новые посты будут публиковаться сразу (без расписания).")

@dp.message(Command(commands=["instantoff"]))
async def instant_off(message: Message):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    cfg = await get_chronos_config()
    new_cfg = ChronosConfig(
        start_hour=cfg.start_hour, end_hour=cfg.end_hour, step_minutes=cfg.step_minutes, instant_publish=False
    )
    await set_chronos_config(new_cfg)
    await run_dynamic_planner(datetime.now(TZ))
    await message.answer("Instant-режим выключен. Новые посты будут планироваться по сетке Хроноса.")

@dp.message(Command(commands=["chronosmode"]))
async def chronos_mode(message: Message, command: CommandObject):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    arg = (command.args or "").strip().lower()
    if arg not in {"dynamic", "static"}:
        current = await get_chronos_mode()
        await message.answer(f"Текущий режим: {current}. Используйте /chronosmode dynamic|static")
        return
    await set_chronos_mode(arg)
    await message.answer(f"Режим Chronos переключен на {arg}.")
    if arg == "dynamic":
        await run_dynamic_planner(datetime.now(TZ))

@dp.message(Command(commands=["predlozhkasetchronos"]))
async def set_chronos_cmd(message: Message, command: CommandObject):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    parts = (command.args or "").split()
    if len(parts) < 3:
        await message.answer(
            "Формат: /predlozhkasetchronos <start_hour 00-23> <end_hour 01-24> <step_minutes 0001-1440> [флаги]\n"
            "Флаги: c — пересобрать все отложенные посты (по умолчанию), nc — не трогать текущее расписание; "
            "i — публиковать сразу (аварийный режим), ni — вернуть планировщик."
        )
        return
    try:
        start = int(parts[0])
        end = int(parts[1])
        step = int(parts[2])
    except ValueError:
        await message.answer("Часы и шаг должны быть числами.")
        return
    flags = [p.lower() for p in parts[3:]]
    collapse = True
    if "nc" in flags:
        collapse = False
    if "c" in flags:
        collapse = True
    cfg_current = await get_chronos_config()
    instant = cfg_current.instant_publish
    if "i" in flags:
        instant = True
    if "ni" in flags:
        instant = False
    start, end, step = _sanitize_config(start, end, step)
    new_cfg = ChronosConfig(start_hour=start, end_hour=end, step_minutes=step, instant_publish=instant)
    await set_chronos_config(new_cfg)
    updated, last_slot = await rebuild_schedule(collapse)
    summary = (
        f"Chronos обновлён: start {start:02d}:00, end {end:02d}:00, шаг {step} мин, instant={'on' if instant else 'off'}.\n"
        f"Пересборка: {'выполнена' if collapse else 'пропущена'}, изменено {updated} постов."
    )
    if last_slot:
        summary += f"\nПоследний слот: {last_slot.astimezone(TZ).strftime('%d/%m %H:%M')}"
    await message.answer(summary)

@dp.message(Command(commands=["schedule"]))
async def schedule_cmd(message: Message):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    text = await format_schedule_view(limit_slots=50)
    await message.answer(text)

@dp.message(Command(commands=["activity"]))
async def activity_chart(message: Message):
    if message.chat.type not in {"private", "supergroup", "group"}:
        return
    path = await get_activity_chart()
    photo = FSInputFile(path)
    await message.answer_photo(photo, caption="Активность предложки")

@dp.message(Command(commands=["radiohead"]))
async def superadmin_help(message: Message):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    await message.answer(
        "Команды суперадмина:\n"
        "/predlozhkasetchronos start end step [c|nc] [i|ni] — настроить сетку Хроноса.\n"
        "   c — пересобрать текущие слоты (default), nc — оставить как есть;\n"
        "   i — публиковать сразу (аварийно), ni — вернуться к расписанию.\n"
        "/schedule — показать ближайшие слоты (до 50 строк).\n"
        "/chronosmode dynamic|static — выбрать планировщик (динамический Chronos или статический).\n"
        "/instanton, /instantoff — включить/выключить мгновенный режим.\n"
        "/botnow — показать текущее время бота.\n"
        "/pausebot, /resumebot — пауза/возобновление работы бота.\n"
        "/superadd id, /superdel id, /superlist — управлять суперадминами.\n"
        "/cancelpost id - снять пост из отложки и отменить.\n"
        "/broadcast текст или ответом - рассылка всем пользователям.\n"
        "/ban_hashtag tag, /unban_hashtag tag - бан/разбан по хэштегу.\n"
        "/backfilldups N [force] - бэкфилл отпечатков и дублей для последних N постов.\n"
        "/backfillchannel N [force] - импорт постов из канала (если нет в БД) + отпечатки.",
    )

@dp.message(Command(commands=["top"]))
@dp.message(F.text == "🏆 Топ")
async def show_top(message: Message):
    # в группах разрешаем команду топ на другие пох
    if message.chat.type != "private":
        month_rows = await db.top_hashtags(days=30, limit=10)
        all_rows = await db.top_hashtags(days=None, limit=10)
        text = format_top_block("🏆 ТОП за 30 дней 🏆", month_rows) + "\n\n" + format_top_block("🏆 ТОП за всё время 🏆", all_rows)
        await message.answer(text)
        return
    month_rows = await db.top_hashtags(days=30, limit=10)
    all_rows = await db.top_hashtags(days=None, limit=10)
    text = format_top_block("🏆 ТОП за 30 дней 🏆", month_rows) + "\n\n" + format_top_block("🏆 ТОП за всё время 🏆", all_rows)
    await message.answer(text, reply_markup=MAIN_MENU)

@dp.message(Command(commands=["myposts"]))
async def my_posts(message: Message):
    if message.chat.type != "private":
        return
    user = await db.get_user_by_tg(message.from_user.id)
    if not user:
        await message.answer("Нажмите /start для начала.", reply_markup=MAIN_MENU)
        return
    rows = await db.list_posts_by_user(user["id"], limit=20)
    if not rows:
        await message.answer("Вы ещё не отправляли посты.", reply_markup=MAIN_MENU)
        return

    def _status_label(status: str) -> str:
        return {
            "pending": "На модерации",
            "scheduled": "Запланирован",
            "published": "Опубликован",
            "rejected": "Отклонён",
        }.get(status, status)

    lines = []
    for r in rows:
        status = r["status"]
        label = _status_label(status)
        ts_raw = r["published_at"] or r["scheduled_at"] or r["approved_at"] or r["created_at"]
        ts_text = "время неизвестно"
        if ts_raw:
            try:
                dt = datetime.fromisoformat(ts_raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=TZ)
                ts_text = format_time(dt)
            except Exception:
                ts_text = ts_raw
        if status == "scheduled":
            time_label = "Запланирован на"
        elif status == "published":
            time_label = "Опубликован"
        elif status == "pending":
            time_label = "Отправлен"
        elif status == "rejected":
            time_label = "Отклонён"
        else:
            time_label = label
        caption_raw = (r["caption"] or "").strip()
        if len(caption_raw) > 80:
            caption_raw = caption_raw[:77] + "..."
        caption_part = "\n  " + escape(caption_raw) if caption_raw else ""
        lines.append(
            f"- #id{r['id']} - {label}\n"
            f"  {time_label}: {ts_text}{caption_part}"
        )
    text = "Ваши последние 20 постов:\n\n" + "\n\n".join(lines)
    await message.answer(text, reply_markup=MAIN_MENU)

@dp.message(F.text == "ℹ️ О боте")
async def about_bot(message: Message):
    if message.chat.type != "private":
        return
    await message.answer("Бот написан с нуля. Вдохновлённый ботом @SilverCumBot", reply_markup=MAIN_MENU)

@dp.message(F.text == "✏️ Изменить хэштег")
async def start_change_hashtag(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return
    user = await db.get_user_by_tg(message.from_user.id)
    if not user:
        await message.answer("Сперва нажмите /start")
        return
    current = user["hashtag"] or "не задан"
    await state.clear()
    await message.answer(
        f"В данный момент вашим хэштегом является #{current}\n"
        "Вы точно хотите его изменить? Чтобы продолжить, нажмите на кнопку \'Далее\'.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Далее", callback_data="tag_change_proceed")],
                [InlineKeyboardButton(text="Отмена", callback_data="tag_change_cancel")],
            ]
        ),
    )




@dp.callback_query(F.data == "tag_change_proceed")
async def tag_change_proceed(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(HashtagFlow.waiting_hashtag)
    await callback.message.edit_text(
        "Введите, пожалуйста, хэштег, который вы будете использовать\n"
        "Разрешено использование маленьких и больших символов кириллицы (а-я А-Я) и латиницы (a-z A-Z), а также цифры (0-9)\n"
        "Использовать символ # не нужно, он автоматически будет добавлен в начало вашего хэштега",
        # стецложка
        reply_markup=None,
    )

@dp.callback_query(F.data == "tag_change_cancel")
async def tag_change_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Отменено.")
    await state.clear()
    await callback.message.edit_text("Изменение хэштега отменено.", reply_markup=None)
    await callback.message.answer("Главное меню:", reply_markup=MAIN_MENU)
@dp.message(HashtagFlow.waiting_hashtag)
async def receive_hashtag(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return
    raw = (message.text or "").strip().lstrip("#")
    if raw.lower() == "cancel" or message.text == "/cancel":
        await state.clear()
        await message.answer("Отменено.", reply_markup=MAIN_MENU)
        return
    if not has_valid_hashtag(raw):
        await message.answer("Нужен один слитный хэштег: буквы/цифры, без пробелов и символов, до 28.")
        return
    user = await db.get_user_by_tg(message.from_user.id)
    if await db.is_hashtag_taken(raw, exclude_tg_id=message.from_user.id):
        await message.answer("Такой хэштег уже занят. Попробуйте другой.")
        return
    await state.update_data(new_hashtag=raw)
    await state.set_state(HashtagFlow.confirm_hashtag)
    await message.answer(
        f"Вы действительно хотите использовать #{raw} в качестве хэштега? Нажмите \'Далее\' для продолжения",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Далее", callback_data="tag_confirm")],
                [InlineKeyboardButton(text="Отмена", callback_data="tag_decline")],
            ]
        ),
    )

@dp.callback_query(HashtagFlow.confirm_hashtag, F.data.in_(["tag_confirm", "tag_decline"]))
async def confirm_hashtag(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    new_tag = data.get("new_hashtag")
    if callback.data == "tag_decline" or not new_tag:
        await state.clear()
        await callback.message.edit_text("Изменение хэштега отменено.", reply_markup=None)
        await callback.message.answer("Главное меню:", reply_markup=MAIN_MENU)
        return
    if await db.is_hashtag_taken(new_tag, exclude_tg_id=callback.from_user.id):
        await callback.message.edit_text("Такой хэштег уже занят. Попробуйте другой.")
        await state.set_state(HashtagFlow.waiting_hashtag)
        return
    await db.set_hashtag(callback.from_user.id, new_tag)
    await state.clear()
    await callback.message.edit_text("?Спасибо, что придумали персональный хэштег", reply_markup=None)
    await callback.message.answer("Главное меню:", reply_markup=MAIN_MENU)
@dp.message(F.text == "📮 Предложить пост")
async def propose_post(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return
    user = await db.get_user_by_tg(message.from_user.id)
    if not user:
        await message.answer("Нажмите /start для начала.")
        return
    if not user["hashtag"]:
        await message.answer("Сначала задайте хэштег.", reply_markup=MAIN_MENU)
        return
    if user["banned"]:
        await message.answer("Вы забанены. Для вопросов - @p0st_shit")
        return
    is_admin_user = await is_admin(message.from_user.id)
    if not is_admin_user:
        pending = await db.get_pending_count(user["id"])
        if pending >= MAX_PENDING_PER_USER:
            await message.answer("Слишком много заявок. Подождите пока модерация разберётся.")
            return
    await state.set_state(SubmissionFlow.waiting_content)
    await state.update_data(is_admin=is_admin_user)
    await message.answer(
        "Отправьте один пост (текст, фото, видео или альбом). После отправки я уточню подтверждение.",
        reply_markup=ReplyKeyboardRemove(),
    )

@dp.message(SubmissionFlow.waiting_content)
async def capture_content(message: Message, state: FSMContext, album: Optional[List[Message]] = None):
    if message.chat.type != "private":
        return
    content = normalize_message_content(message, album=album)
    if not content:
        await message.answer("Не удалось понять контент. Пришлите текст, фото, видео или альбом.")
        return

    await state.update_data(draft=json.dumps(content.__dict__))
    await state.set_state(SubmissionFlow.confirm_content)
    text = (
        "Вы действительно хотите отправить этот пост? Нажмите 'Далее' для отправки поста администрации.\n\n"
        "Важно: если такой же пост уже публиковался, заявка будет отклонена."
    )
    await message.answer(
        text,
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="Далее", callback_data="confirm_send")],
                [InlineKeyboardButton(text="Отмена", callback_data="cancel_send")],
            ]
        ),
    )

@dp.callback_query(SubmissionFlow.confirm_content, F.data.in_(["confirm_send", "cancel_send"]))
async def confirm_send(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if callback.data == "cancel_send":
        await state.clear()
        await callback.message.edit_text("Отменено.", reply_markup=None)
        await callback.message.answer("Главное меню:", reply_markup=MAIN_MENU)
        return
    data = await state.get_data()
    draft_raw = data.get("draft")
    is_admin_sender = data.get("is_admin")
    if is_admin_sender is None:
        is_admin_sender = await is_admin(callback.from_user.id)
    if not draft_raw:
        await callback.message.edit_text("Нет подготовленного поста.")
        await state.clear()
        return
    draft_dict = json.loads(draft_raw)
    content = DraftContent(**draft_dict)
    user = await db.get_user_by_tg(callback.from_user.id)
    if not user or not user["hashtag"]:
        await callback.message.edit_text("Сначала задайте хэштег.")
        await state.clear()
        return
    post_id = await db.create_post(
        user_id=user["id"],
        media_type=content.kind,
        caption=content.caption or "",
        media_json=json.dumps(content.__dict__),
        status="pending",
    )
    hashtag = user["hashtag"]
    username = user["username"] or ""

    dup_fast_matches: List[Dict[str, Any]] = []
    try:
        dup_fast_matches.extend(await detect_duplicate_images_fast(content))
    except Exception as e:
        logger.warning("Fast duplicate image check failed: %s", e)
    try:
        dup_fast_matches.extend(await detect_duplicate_videos_fast(content))
    except Exception as e:
        logger.warning("Fast duplicate video check failed: %s", e)

    dup_info: Optional[str] = None
    deep_task: Optional[asyncio.Task[Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]]] = None
    deep_pending = False
    if content_has_images(content) or content_has_videos(content):
        deep_task = asyncio.create_task(compute_duplicate_result_deep(content))
        try:
            image_fps, video_fps, dup_deep_matches = await asyncio.wait_for(
                asyncio.shield(deep_task), timeout=max(0.1, DUPLICATE_SYNC_TIMEOUT_SECONDS)
            )
            if image_fps or video_fps:
                try:
                    if image_fps:
                        await db.add_image_fingerprints(post_id, image_fps)
                    if video_fps:
                        await db.add_video_fingerprints(post_id, video_fps)
                except Exception as e:
                    logger.warning("Failed to save fingerprints for post %s: %s", post_id, e)
                dup_info = format_duplicate_info(dup_deep_matches, always_show=True)
            else:
                dup_info = "Повторки: не удалось проверить"
            try:
                await db.set_post_duplicate_info(post_id, dup_info)
            except Exception as e:
                logger.warning("Failed to store duplicate info for post %s: %s", post_id, e)
        except asyncio.TimeoutError:
            deep_pending = True
            if dup_fast_matches:
                dup_info = format_duplicate_info(dup_fast_matches, always_show=True)
            else:
                dup_info = format_duplicate_info(None, pending=True, always_show=True)
            try:
                await db.set_post_duplicate_info(post_id, dup_info)
            except Exception as e:
                logger.warning("Failed to store duplicate info placeholder for post %s: %s", post_id, e)
    caption_parts = []
    if content.caption:
        caption_parts.append(escape(content.caption))
    caption_parts.append(f"#{escape(hashtag)}")
    caption_parts.append(f"ID поста - #id{post_id}")
    if dup_info:
        caption_parts.append(escape(dup_info))
    if username:
        caption_parts.append(f"Автор: @{escape(username)}")
    else:
        caption_parts.append(f"Автор: {callback.from_user.id}")
    caption = truncate_caption("\n\n".join(caption_parts))
    if is_admin_sender:
        try:
            scheduled_dt = await schedule_post(post_id)
        except Exception as e:
            logger.error("Failed to auto-schedule admin post %s: %s", post_id, e)
            if deep_task and not deep_task.done():
                deep_task.cancel()
            await callback.message.edit_text("Не удалось поставить пост в расписание.", reply_markup=None)
            await state.clear()
            return
        if deep_pending and deep_task:
            asyncio.create_task(finalize_duplicate_check_for_post(post_id, deep_task))
        await state.clear()
        await callback.message.edit_text(
            "Пост от администратора принят без голосования и поставлен в расписание.",
            reply_markup=None,
        )
        await notify_user_status(post_id, "scheduled", None, scheduled_dt.isoformat())
        await callback.message.answer("Главное меню:", reply_markup=MAIN_MENU)
        return
    likes, dislikes = await db.get_vote_counts(post_id)
    markup = build_inline_keyboard(post_id, likes, dislikes, await db.count_ban_votes(user["id"]))
    try:
        message_id, message_ids = await send_content_copy(
            ADMIN_CHAT_ID,
            content,
            caption=caption,
            reply_markup=markup,
            force_buttons_message=content.kind == "album",
        )
        await db.update_post_admin_messages(post_id, message_id, message_ids)
    except Exception as e: 
        logger.error("Failed to forward to admin chat: %s", e)
        if deep_task and not deep_task.done():
            deep_task.cancel()
        await callback.message.edit_text("Не удалось отправить в админ-чат.")
        await state.clear()
        return

    if deep_pending and deep_task:
        asyncio.create_task(finalize_duplicate_check_for_post(post_id, deep_task))

    await callback.message.edit_text("Пост отправлен на модерацию.", reply_markup=None)
    await state.clear()
    await callback.message.answer(
        "Ваш пост поступил, спасибо вам за предложку!\n"
        "Пожалуйста, ожидайте решение демократичной администрации о публикации\n"
        f"ID поста - #id{post_id}\n"
        ""
        "Может ещё что-нибудь скинешь?\n😉",
        # нет
        reply_markup=MAIN_MENU,
    )

async def update_admin_view(post_id: int):
    post = await db.get_post(post_id)
    if not post or not post["admin_message_id"]:
        return
    user = await db.get_user_by_id(post["user_id"])
    hashtag = user["hashtag"] if user else ""
    author = f"@{user['username']}" if user and user["username"] else str(user["tg_id"]) if user else ""
    likes, dislikes = await db.get_vote_counts(post_id)
    reason = post["reason"]
    status = post["status"]
    ban_count = await db.count_ban_votes(user["id"]) if user else 0
    markup = build_inline_keyboard(post_id, likes, dislikes, ban_count)
    admin_message_id = post["admin_message_id"]
    caption = format_admin_caption(
        post["caption"] or "",
        hashtag,
        post_id,
        likes,
        dislikes,
        status,
        post["scheduled_at"],
        reason,
        author,
        post["duplicate_info"],
    )
    try:
        if post["media_type"] in {"photo", "video", "animation", "document", "audio"}:
            await bot.edit_message_caption(
                chat_id=ADMIN_CHAT_ID,
                message_id=admin_message_id,
                caption=caption,
                reply_markup=markup,
            )
        else:
            await bot.edit_message_text(
                caption,
                chat_id=ADMIN_CHAT_ID,
                message_id=admin_message_id,
                reply_markup=markup,
            )
    except Exception as e:
        if "message is not modified" in str(e):
            logger.debug("Admin message %s unchanged", post_id)
        else:
            logger.warning("Failed to update admin message %s: %s", post_id, e)

async def notify_user_status(post_id: int, status: str, reason: Optional[str], scheduled_at: Optional[str]):
    post = await db.get_post(post_id)
    if not post:
        return
    if post["notified_status"] == status:
        return
    user = await db.get_user_by_id(post["user_id"])
    if not user:
        return
    chat_id = user["tg_id"]
    if status == "rejected":
        text = (
            "К сожалению, ваш пост отклонили!\n"
            f"ID поста - #id{post_id}\n"
            "Eсли вы или кто-то из ваших знакомых подумывает о самоубийстве, пожалуйста, не стесняйтесь позвонить\n"
            "Россия: +78002000122\n"
            "Не Россия: https://en.wikipedia.org/wiki/List_of_suicide_crisis_lines\n\n"
            "Если вы хотите обсудить данное решение с администрацией, то, пожалуйста, напишите @p0st_shit\n"
            "Мы всё равно благодарны вам за уделённое время"
        )
        # не знаю будет ли смешно но увидел что на некоторых сабредитах такое пишут когда
        # игру отменяют или что-то вроде того
        # хотя едиственный возможный суицид который может произойти это мой из-за этого языка
        if reason:
            text = (
                "К сожалению, ваш пост отклонили!\n"
                f"ID поста - #id{post_id}\n"
                "Eсли вы или кто-то из ваших знакомых подумывает о самоубийстве, пожалуйста, не стесняйтесь позвонить\n"
                "Россия: +78002000122\n"
                "Не Россия: https://en.wikipedia.org/wiki/List_of_suicide_crisis_lines\n\n"
                "Если вы хотите обсудить данное решение с администрацией, то, пожалуйста, напишите @p0st_shit\n"
                "Мы всё равно благодарны вам за уделённое время"
            )
        await bot.send_message(chat_id, text)
    elif status == "scheduled":
        when = format_time(datetime.fromisoformat(scheduled_at)) if scheduled_at else "скоро"
        await bot.send_message(
            chat_id,
            "Ваш пост приняли!\n"
            f"ID поста - #id{post_id}\n"
            f"Планируемое время публикации:\n{when}\n"
            "Спасибо большое за ваш вклад.",
        )
    await db.set_notified_status(post_id, status)

async def evaluate_post(post_id: int):
    post = await db.get_post(post_id)
    if not post:
        return
    if post["status"] == "published":
        return
    # интересно эти комментрии будет кто-то читать?
    likes, dislikes = await db.get_vote_counts(post_id)
    total = likes + dislikes
    status = post["status"]
    target_status = status
    scheduled_at = post["scheduled_at"]
    reason = post["reason"]

    if total >= MIN_VOTES_FOR_DECISION:
        if likes > dislikes:
            target_status = "scheduled"
            scheduled_dt = await schedule_post(post_id)
            scheduled_at = scheduled_dt.isoformat()
        else:
            target_status = "rejected"
            await db.set_post_status(post_id, "rejected")
    if target_status != status:
        await notify_user_status(post_id, target_status, reason, scheduled_at)
    await update_admin_view(post_id)

    # я в рот ебал этого питона джава легче

@dp.callback_query(F.data.startswith("vote:"))
async def handle_vote(callback: CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) != 3:
        with contextlib.suppress(Exception):
            await callback.answer()
        return
    post_id = int(parts[1])
    value = parts[2]
    if value not in {"like", "dislike"}:
        with contextlib.suppress(Exception):
            await callback.answer()
        return
    if not await is_admin(callback.from_user.id):
        with contextlib.suppress(Exception):
            await callback.answer("Только админы могут голосовать.", show_alert=True)
        return
    await db.toggle_vote(post_id, callback.from_user.id, value)
    with contextlib.suppress(Exception):
        await callback.answer("Голос учтён.")
    await evaluate_post(post_id)

@dp.callback_query(F.data.startswith("ban:"))
async def handle_ban_vote(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        with contextlib.suppress(Exception):
            await callback.answer("Только админы могут голосовать.", show_alert=True)
        return
    parts = callback.data.split(":")
    post_id = int(parts[1])
    post = await db.get_post(post_id)
    if not post:
        with contextlib.suppress(Exception):
            await callback.answer()
        return
    user_row = await db.get_user_by_id(post["user_id"])
    if not user_row:
        with contextlib.suppress(Exception):
            await callback.answer()
        return
    votes = await db.toggle_ban_vote(user_row["id"], callback.from_user.id)
    if votes >= BAN_THRESHOLD:
        await db.mark_banned(user_row["id"], True)
        await db.clear_ban_votes(user_row["id"])
        try:
            await bot.send_message(user_row["tg_id"], "Вы забанены, обратитесь к @p0st_shit")
        except Exception:
            pass
    with contextlib.suppress(Exception):
        await callback.answer("Голос за бан обновлён.")
    await update_admin_view(post_id)

@dp.callback_query(F.data.startswith("reason:"))
async def start_reason(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        return await callback.answer("Только админы могут добавлять причину.", show_alert=True)
    parts = callback.data.split(":")
    if len(parts) != 2:
        return await callback.answer()
    post_id = int(parts[1])
    await callback.answer()
    prompt = await callback.message.reply("Напишите причину отклонения (до 800 символов) ответом на это сообщение.")
    pending_reasons[callback.from_user.id] = {"post_id": post_id, "prompt_id": prompt.message_id}

@dp.message()
async def catch_reason(message: Message):
    record = pending_reasons.get(message.from_user.id)
    if not record:
        return
    expected_reply = record.get("prompt_id")
    if not message.reply_to_message or message.reply_to_message.message_id != expected_reply:
        return
    post_id = record["post_id"]
    pending_reasons.pop(message.from_user.id, None)
    text = (message.text or "").strip()
    if not text:
        await message.reply("Пустая причина не сохранена.")
        return
    reason = text[:MAX_REASON_LEN]
    await db.set_reason(post_id, reason)
    await message.reply("Причина сохранена.")
    await update_admin_view(post_id)
    post = await db.get_post(post_id)
    if post and post["status"] == "rejected":
        await notify_user_status(post_id, "rejected", reason, post["scheduled_at"])

async def main():
    await db.connect()
    scheduler = asyncio.create_task(scheduler_loop())
    try:
        await dp.start_polling(bot)
    finally:
        scheduler.cancel()
        with contextlib.suppress(Exception):
            await scheduler
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())

