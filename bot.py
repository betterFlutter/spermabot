import asyncio
import contextlib
import json
import logging
import os
import re
import math
import shutil
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
MIN_VOTES_FOR_DECISION = 1
MAX_REASON_LEN = 800
MAX_CAPTION_LEN = 800
MAX_PENDING_PER_USER = 100
DB_PATH = os.getenv("DB_PATH", os.path.join("data", "bot.db"))
TZ = timezone(timedelta(hours=TZ_OFFSET_HOURS))

MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📮Предложить пост")],
        [KeyboardButton(text="🌚Изменить хэштег")],
        [KeyboardButton(text="🏆Топ")],
        [KeyboardButton(text="ℹ️О боте")],
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
            CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status);
            CREATE INDEX IF NOT EXISTS idx_posts_scheduled_at ON posts(scheduled_at);
            """
        )
        # миграции
        cols = {row["name"] for row in await (await self.db.execute("PRAGMA table_info(posts)")).fetchall()}
        if "approved_at" not in cols:
            await self.db.execute("ALTER TABLE posts ADD COLUMN approved_at TEXT")
        if "published_at" not in cols:
            await self.db.execute("ALTER TABLE posts ADD COLUMN published_at TEXT")
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

    async def update_post_admin_messages(self, post_id: int, message_id: int, message_ids: List[int]):
        await self.db.execute(
            "UPDATE posts SET admin_message_id=?, admin_message_ids=?, admin_chat_id=? WHERE id=?",
            (message_id, json.dumps(message_ids), ADMIN_CHAT_ID, post_id),
        )
        await self.db.commit()

    async def get_post(self, post_id: int):
        cur = await self.db.execute("SELECT * FROM posts WHERE id=?", (post_id,))
        return await cur.fetchone()

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
            [InlineKeyboardButton(text="✏ Причина", callback_data=f"reason:{post_id}")],
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
) -> str:
    parts = []
    content = base_caption.strip()
    if content:
        parts.append(escape(content))
    parts.append(f"#{escape(hashtag or '')}")
    parts.append(f"ID поста - #id{post_id}")
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
                items.append({"type": "photo", "file_id": msg.photo[-1].file_id, "caption": msg.caption})
            elif msg.video:
                items.append({"type": "video", "file_id": msg.video.file_id, "caption": msg.caption})
            elif msg.document:
                items.append({"type": "document", "file_id": msg.document.file_id, "caption": msg.caption})
            elif msg.audio:
                items.append({"type": "audio", "file_id": msg.audio.file_id, "caption": msg.caption})
        return DraftContent(kind="album", items=items, caption=caption or "")
    if message.photo:
        return DraftContent(
            kind="photo", items=[{"type": "photo", "file_id": message.photo[-1].file_id}], caption=message.caption or ""
        )
    if message.video:
        return DraftContent(
            kind="video", items=[{"type": "video", "file_id": message.video.file_id}], caption=message.caption or ""
        )
    if message.animation:
        return DraftContent(
            kind="animation", items=[{"type": "animation", "file_id": message.animation.file_id}], caption=message.caption or ""
        )
    if message.document:
        return DraftContent(
            kind="document", items=[{"type": "document", "file_id": message.document.file_id}], caption=message.caption or ""
        )
    if message.audio:
        return DraftContent(kind="audio", items=[{"type": "audio", "file_id": message.audio.file_id}], caption=message.caption or "")
    if message.voice:
        return DraftContent(kind="voice", items=[{"type": "voice", "file_id": message.voice.file_id}], caption="")
    if message.video_note:
        return DraftContent(kind="video_note", items=[{"type": "video_note", "file_id": message.video_note.file_id}], caption="")
    if message.text:
        return DraftContent(kind="text", items=[], caption=message.text)
    return None

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
        # выбрать пост этого автора
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
    # разрешаем в ЛС и группах
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
        "/cancelpost id — снять пост из отложки и отменить.\n"
        "/broadcast текст или ответом — рассылка всем пользователям.\n"
        "/ban_hashtag tag, /unban_hashtag tag — бан/разбан по хэштегу.",
    )

@dp.message(Command(commands=["top"]))
@dp.message(F.text == "🏆Топ")
async def show_top(message: Message):
    # в группах разрешаем команду топ на другие пох
    if message.chat.type != "private":
        month_rows = await db.top_hashtags(days=30, limit=10)
        all_rows = await db.top_hashtags(days=None, limit=10)
        text = format_top_block("🗿 ТОП за 30 дней 🗿", month_rows) + "\n\n" + format_top_block("🗿 ТОП за всё время 🗿", all_rows)
        await message.answer(text)
        return
    month_rows = await db.top_hashtags(days=30, limit=10)
    all_rows = await db.top_hashtags(days=None, limit=10)
    text = format_top_block("🗿 ТОП за 30 дней 🗿", month_rows) + "\n\n" + format_top_block("🗿 ТОП за всё время 🗿", all_rows)
    await message.answer(text, reply_markup=MAIN_MENU)

@dp.message(F.text == "ℹ️О боте")
async def about_bot(message: Message):
    if message.chat.type != "private":
        return
    await message.answer("Бот написан с нуля. Вдохновлённый ботом @SilverCumBot", reply_markup=MAIN_MENU)

@dp.message(F.text == "🌚Изменить хэштег")
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
    await callback.message.edit_text("✅Спасибо, что придумали персональный хэштег", reply_markup=None)
    await callback.message.answer("Главное меню:", reply_markup=MAIN_MENU)
@dp.message(F.text == "📮Предложить пост")
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
        await message.answer("Вы забанены. Для вопросов — @p0st_shit")
        return
    pending = await db.get_pending_count(user["id"])
    if pending >= MAX_PENDING_PER_USER:
        await message.answer("Слишком много заявок. Подождите пока модерация разберётся.")
        return
    await state.set_state(SubmissionFlow.waiting_content)
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
    await message.answer(
        "Вы действительно хотите отправить этот пост? Нажмите 'Далее' для отправки поста администрации",
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
    caption_parts = []
    if content.caption:
        caption_parts.append(escape(content.caption))
    caption_parts.append(f"#{escape(hashtag)}")
    caption_parts.append(f"ID поста - #id{post_id}")
    if username:
        caption_parts.append(f"Автор: @{escape(username)}")
    else:
        caption_parts.append(f"Автор: {callback.from_user.id}")
    caption = truncate_caption("\n\n".join(caption_parts))
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
        await callback.message.edit_text("Не удалось отправить в админ-чат.")
        await state.clear()
        return
    await callback.message.edit_text("Пост отправлен на модерацию.", reply_markup=None)
    await state.clear()
    await callback.message.answer(
        "Ваш пост поступил, спасибо вам за предложку!\n"
        "Пожалуйста, ожидайте решение демократичной администрации о публикации\n"
        f"ID поста - #id{post_id}\n"
        ""
        "Может ещё что-нибудь скинешь?\n👉👈",
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
