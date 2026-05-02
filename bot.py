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
import textwrap
import random
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite
from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.filters.command import CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage, SimpleEventIsolation
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    FSInputFile,
    BufferedInputFile,
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
from PIL import Image, ImageFilter, UnidentifiedImageError, ImageDraw, ImageFont, ImageOps
import pytesseract
try:
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from telethon import functions as tl_functions, utils as tl_utils
except Exception:
    TelegramClient = None
    StringSession = None
    tl_functions = None
    tl_utils = None

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
DISCUSSION_CHAT_ID = int(os.getenv("DISCUSSION_CHAT_ID", str(ADMIN_CHAT_ID)))
SUPER_ADMIN_ID = int(os.getenv("SUPER_ADMIN_ID", "583781734"))
TZ_OFFSET_HOURS = int(os.getenv("TZ_OFFSET_HOURS", "3"))
CHRONOS_START_HOUR_DEFAULT = int(os.getenv("CHRONOS_START_HOUR", "6"))
CHRONOS_END_HOUR_DEFAULT = int(os.getenv("CHRONOS_END_HOUR", "24"))
CHRONOS_STEP_MINUTES_DEFAULT = int(os.getenv("CHRONOS_STEP_MINUTES", "120"))
INSTANT_PUBLISH_DEFAULT = os.getenv("INSTANT_PUBLISH", "false").lower() in {"1", "true", "yes", "on"}
CHRONOS_MODE_DEFAULT = os.getenv("CHRONOS_MODE", "dynamic")
BOT_PAUSED_DEFAULT = False
BAN_THRESHOLD = 4
MIN_VOTES_FOR_DECISION = 2
MAX_REASON_LEN = 800
MAX_ADMIN_CAPTION_LEN = 800
MAX_TEXT_POST_LEN = 3500
MAX_MEDIA_POST_LEN = 1024
PUBLISH_LINK_URL = "https://t.me/goldencumbot"
PUBLISH_LINK_LABEL = "Goldencum. Скинуть мем"
PUBLIC_POST_VIEW_LIMIT = int(os.getenv("PUBLIC_POST_VIEW_LIMIT", "10"))
PUBLIC_POST_VIEW_TIMEOUT_SECONDS = int(os.getenv("PUBLIC_POST_VIEW_TIMEOUT_SECONDS", "300"))
SUBMIT_DUPLICATE_SCAN_EXPECTED_SECONDS = float(os.getenv("SUBMIT_DUPLICATE_SCAN_EXPECTED_SECONDS", "12"))
SUBMIT_DUPLICATE_SCAN_UPDATE_SECONDS = float(os.getenv("SUBMIT_DUPLICATE_SCAN_UPDATE_SECONDS", "1"))
ADMIN_SUBMIT_CONFIRM_TEXT = (
    "Проверка повторок завершена. Похожих опубликованных постов не нашёл.\n\n"
    "Нажмите 'Далее', чтобы отправить пост администрации."
)
ADMIN_SUBMIT_CONFIRM_DUPLICATE_TEXT = (
    "Я нашёл возможную повторку. Проверьте список ниже перед отправкой. "
    "Если это тот же мем или почти тот же пост, лучше нажать 'Отмена': такую заявку, скорее всего, отклонят. "
    "Если поиск ошибся, можно нажать 'Далее' и отправить пост администрации."
)
ADMIN_SUBMIT_CONFIRM_SCAN_FAILED_TEXT = (
    "Не смог надёжно проверить повторки из-за ошибки загрузки или обработки.\n\n"
    "Можно нажать 'Далее', но лучше отправлять только если вы уверены, что такого поста ещё не было."
)
ADMIN_SUBMIT_CONFIRM_NO_SCAN_TEXT = (
    "Пост готов к отправке администрации.\n\n"
    "Нажмите 'Далее', чтобы отправить пост."
)
PUBLIC_DUPLICATE_NOTICE_LABEL = "Показать пост"
ADMIN_DUPLICATE_NOTICE_LABEL = "Расширенная информация для админов"
MAX_PENDING_PER_USER = 100
RATE_LIMIT_MAX_POSTS = int(os.getenv("RATE_LIMIT_MAX_POSTS", "50"))
RATE_LIMIT_WINDOW_MINUTES = int(os.getenv("RATE_LIMIT_WINDOW_MINUTES", "30"))
ACTIVITY_SMOOTH_SIGMA = float(os.getenv("ACTIVITY_SMOOTH_SIGMA", "0.6"))
ACTIVITY_AMPLIFY = float(os.getenv("ACTIVITY_AMPLIFY", "1.2"))
DB_PATH = os.getenv("DB_PATH", os.path.join("data", "bot.db"))
TZ = timezone(timedelta(hours=TZ_OFFSET_HOURS))

# переменные для Мнемосины. настроить если есть проблемы, но дефолты в целом норм

DUPLICATE_IMAGE_HASH_SIZE           = int(os.getenv("DUPLICATE_IMAGE_HASH_SIZE", "8"))
DUPLICATE_PHASH_HIGHFREQ_SIZE       = max(DUPLICATE_IMAGE_HASH_SIZE * 4, int(os.getenv("DUPLICATE_PHASH_HIGHFREQ_SIZE", "32")))
DUPLICATE_WHASH_IMAGE_SIZE          = max(DUPLICATE_IMAGE_HASH_SIZE * 4, int(os.getenv("DUPLICATE_WHASH_IMAGE_SIZE", "32")))
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
DUPLICATE_GAUSSIAN_BLUR_RADIUS      = float(os.getenv("DUPLICATE_GAUSSIAN_BLUR_RADIUS", "0.25"))
DUPLICATE_IMAGE_DOWNLOAD_TIMEOUT_SECONDS = float(os.getenv("DUPLICATE_IMAGE_DOWNLOAD_TIMEOUT_SECONDS", "15"))
DUPLICATE_IMAGE_DOWNLOAD_RETRIES    = int(os.getenv("DUPLICATE_IMAGE_DOWNLOAD_RETRIES", "3"))
DUPLICATE_IMAGE_DOWNLOAD_RETRY_DELAY_SECONDS = float(os.getenv("DUPLICATE_IMAGE_DOWNLOAD_RETRY_DELAY_SECONDS", "0.7"))
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
DUPLICATE_GEOMETRY_ENABLED          = os.getenv("DUPLICATE_GEOMETRY_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
DUPLICATE_GEOMETRY_TIMEOUT_SECONDS  = float(os.getenv("DUPLICATE_GEOMETRY_TIMEOUT_SECONDS", "12"))
DUPLICATE_GEOMETRY_MAX_MATCHES_PER_ITEM = int(os.getenv("DUPLICATE_GEOMETRY_MAX_MATCHES_PER_ITEM", "4"))
DUPLICATE_SIFT_FEATURE_VERSION      = int(os.getenv("DUPLICATE_SIFT_FEATURE_VERSION", "1"))
DUPLICATE_SIFT_TOPK                 = int(os.getenv("DUPLICATE_SIFT_TOPK", "12"))
DUPLICATE_SIFT_TOPK_SIZE            = int(os.getenv("DUPLICATE_SIFT_TOPK_SIZE", "4"))
DUPLICATE_SIFT_MAX_DIM              = int(os.getenv("DUPLICATE_SIFT_MAX_DIM", "900"))
DUPLICATE_SIFT_NFEATURES            = int(os.getenv("DUPLICATE_SIFT_NFEATURES", "1200"))
DUPLICATE_SIFT_CONTRAST_THRESHOLD   = float(os.getenv("DUPLICATE_SIFT_CONTRAST_THRESHOLD", "0.03"))
DUPLICATE_SIFT_RATIO                = float(os.getenv("DUPLICATE_SIFT_RATIO", "0.75"))
DUPLICATE_SIFT_MUTUAL               = os.getenv("DUPLICATE_SIFT_MUTUAL", "true").lower() in {"1", "true", "yes", "on"}
DUPLICATE_SIFT_MIN_GOOD             = int(os.getenv("DUPLICATE_SIFT_MIN_GOOD", "18"))
DUPLICATE_SIFT_MIN_INLIERS          = int(os.getenv("DUPLICATE_SIFT_MIN_INLIERS", "12"))
DUPLICATE_SIFT_MIN_INLIER_RATIO     = float(os.getenv("DUPLICATE_SIFT_MIN_INLIER_RATIO", "0.30"))
DUPLICATE_SIFT_RANSAC_REPROJ        = float(os.getenv("DUPLICATE_SIFT_RANSAC_REPROJ", "4.0"))
DUPLICATE_SIFT_MAX_RMSE             = float(os.getenv("DUPLICATE_SIFT_MAX_RMSE", "4.0"))
DUPLICATE_SIFT_MIN_COVERAGE         = float(os.getenv("DUPLICATE_SIFT_MIN_COVERAGE", "0.10"))
DUPLICATE_SIFT_MIN_SECONDARY_COVERAGE = float(os.getenv("DUPLICATE_SIFT_MIN_SECONDARY_COVERAGE", "0.015"))
DUPLICATE_ASIFT_ENABLED             = os.getenv("DUPLICATE_ASIFT_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
DUPLICATE_ASIFT_TOPK                = int(os.getenv("DUPLICATE_ASIFT_TOPK", "1"))
DUPLICATE_ASIFT_MAX_HASH_SCORE      = int(os.getenv("DUPLICATE_ASIFT_MAX_HASH_SCORE", "16"))
DUPLICATE_ASIFT_TILTS               = _parse_float_list(os.getenv("DUPLICATE_ASIFT_TILTS", "1.0,1.5"))
DUPLICATE_ASIFT_ROTATION_DEGREES    = _parse_float_list(os.getenv("DUPLICATE_ASIFT_ROTATION_DEGREES", "0,20,-20"))
DUPLICATE_ASIFT_VIEW_LIMIT          = int(os.getenv("DUPLICATE_ASIFT_VIEW_LIMIT", "4"))
DUPLICATE_ASIFT_MAX_FEATURES        = int(os.getenv("DUPLICATE_ASIFT_MAX_FEATURES", "1600"))

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
DUPLICATE_VIDEO_MATCH_RATIO         = float(os.getenv("DUPLICATE_VIDEO_MATCH_RATIO", "0.55"))
DUPLICATE_VIDEO_MATCH_MIN           = int(os.getenv("DUPLICATE_VIDEO_MATCH_MIN", "3"))
DUPLICATE_VIDEO_TIME_TOLERANCE      = float(os.getenv("DUPLICATE_VIDEO_TIME_TOLERANCE", "0.03"))
DUPLICATE_VIDEO_TIME_TOLERANCE_SECONDS = float(os.getenv("DUPLICATE_VIDEO_TIME_TOLERANCE_SECONDS", "0.6"))
DUPLICATE_VIDEO_TIME_SHIFTS         = _parse_float_list(os.getenv("DUPLICATE_VIDEO_TIME_SHIFTS", "0,0.04,-0.04,0.08,-0.08"))
DUPLICATE_VIDEO_TIME_SHIFTS_SECONDS = _parse_float_list(os.getenv("DUPLICATE_VIDEO_TIME_SHIFTS_SECONDS", "0,0.5,-0.5,1,-1,2,-2"))
DUPLICATE_VIDEO_TIME_SHIFT_LIMIT    = int(os.getenv("DUPLICATE_VIDEO_TIME_SHIFT_LIMIT", "25"))
DUPLICATE_VIDEO_TIME_BINS           = int(os.getenv("DUPLICATE_VIDEO_TIME_BINS", "3"))
DUPLICATE_VIDEO_DHASH_THRESHOLD     = int(os.getenv("DUPLICATE_VIDEO_DHASH_THRESHOLD", "22"))
DUPLICATE_VIDEO_PHASH_THRESHOLD     = int(os.getenv("DUPLICATE_VIDEO_PHASH_THRESHOLD", "24"))
DUPLICATE_VIDEO_WHASH_THRESHOLD     = int(os.getenv("DUPLICATE_VIDEO_WHASH_THRESHOLD", "26"))
DUPLICATE_VIDEO_SINGLE_HASH_THRESHOLD = int(os.getenv("DUPLICATE_VIDEO_SINGLE_HASH_THRESHOLD", "5"))
DUPLICATE_VIDEO_FRAME_AVG_DIST_MAX  = float(os.getenv("DUPLICATE_VIDEO_FRAME_AVG_DIST_MAX", "20"))
DUPLICATE_VIDEO_FRAME_MAX_DIST      = int(os.getenv("DUPLICATE_VIDEO_FRAME_MAX_DIST", "30"))
DUPLICATE_VIDEO_MAX_MATCHES_PER_ITEM = int(os.getenv("DUPLICATE_VIDEO_MAX_MATCHES_PER_ITEM", "8"))
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
CUSTOM_FONT_PATH                    = os.getenv("CUSTOM_FONT_PATH")
MEME_TRANSLATE_ENABLED              = os.getenv("MEME_TRANSLATE_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
ENGLISH_SHARE_THRESHOLD             = float(os.getenv("ENGLISH_SHARE_THRESHOLD", "0.5"))
GOOGLE_TRANSLATE_ENABLED            = os.getenv("GOOGLE_TRANSLATE_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
OCR_ENABLED                         = os.getenv("OCR_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
OCR_LANG                            = os.getenv("OCR_LANG", "eng")
OCR_MIN_CONF                        = float(os.getenv("OCR_MIN_CONF", "40"))
OCR_PSM                             = os.getenv("OCR_PSM", "6")
TESSERACT_CMD                        = os.getenv("TESSERACT_CMD", "")
WATERMARK_ENABLED                   = os.getenv("WATERMARK_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
WATERMARK_PATH                      = os.getenv("WATERMARK_PATH", "watermark.png")
WATERMARK_SCALE                     = float(os.getenv("WATERMARK_SCALE", "0.25"))
WATERMARK_MARGIN_PCT                = float(os.getenv("WATERMARK_MARGIN_PCT", "0.03"))
WATERMARK_BLUR_PX                   = float(os.getenv("WATERMARK_BLUR_PX", "0.8"))
WATERMARK_POSITION                  = os.getenv("WATERMARK_POSITION", "br")
WATERMARK_TEXT                      = os.getenv("WATERMARK_TEXT", "")
WATERMARK_TEXT_WIDTH_PCT            = float(os.getenv("WATERMARK_TEXT_WIDTH_PCT", "0.25"))
WATERMARK_TEXT_SIZE_PCT             = float(os.getenv("WATERMARK_TEXT_SIZE_PCT", "0.05"))
WATERMARK_ROTATION_DEG              = float(os.getenv("WATERMARK_ROTATION_DEG", "10.0"))
WATERMARK_FONT_PATH                 = os.getenv("WATERMARK_FONT_PATH", "")
WATERMARK_ALPHA                     = float(os.getenv("WATERMARK_ALPHA", "0.35"))
WATERMARK_COLOR                     = os.getenv("WATERMARK_COLOR", "auto")

discussion_map: Dict[int, int] = {}
discussion_waiters: Dict[int, List[asyncio.Future]] = defaultdict(list)
last_prompt_message: Dict[int, int] = {}  # chat_id -> message_id для "Скинуть мем"
last_submit_control_message: Dict[int, int] = {}  
public_post_view_limits: Dict[int, Dict[str, Any]] = {}
last_forward_message: Dict[int, int] = {}  # chat_id -> последний message_id с канала
last_channel_message: Dict[int, int] = {}  # chat_id -> последний msg в чате от каналА

VPS_CONFIGS_CACHE_DIR = os.path.join("data", "cache", "vps-configs")
VPS_REQUIRED_PUBLISHED_POSTS = 1
VPS_REQUIRED_DAYS = 30
VPS_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]*://")
VPS_DISCLAIMER_TEXT = (
    "⚠️ <b>Дисклеймер</b>\n"
    "1) Администратор и разработчики бота не являются авторами, владельцами, операторами, правообладателями или поставщиками указанных сетевых профилей.\n"
    "2) Бот не является первоисточником: данные автоматически извлекаются из открытых публичных материалов и отображаются без независимой технической и правовой экспертизы.\n"
    "3) Бот не оказывает услуги связи, не предоставляет доступ к каналам передачи данных, не выполняет функцию провайдера и не заключает с пользователем договоров на сетевые услуги.\n"
    "4) В рамках данного раздела отсутствуют продажа доступа, подписка, коммерческое сопровождение, гарантия результата, техническая поддержка и обязательства по доступности сервиса.\n"
    "5) Размещение сведений носит исключительно справочный/информационный характер и не является рекламой, продвижением, рекомендацией, побуждением к действиям или публичной офертой.\n"
    "6) В материалах раздела не применяются реферальные механики, промокоды, обещания выгоды, заявления о превосходстве, гарантии скорости/стабильности или иные рекламные атрибуты.\n"
    "7) Работоспособность, безопасность, актуальность, корректность, совместимость, законность происхождения и правовой статус публикуемых данных не гарантируются.\n"
    "8) Администратор бота не контролирует содержание сторонних источников, не отвечает за их изменения, удаление, недоступность, ограничения доступа и возможный вред от использования данных.\n"
    "9) Пользователь самостоятельно и на свой риск принимает решения об использовании полученной информации, а также самостоятельно оценивает технические и правовые последствия.\n"
    "10) Пользователь обязан соблюдать применимое законодательство, условия своих операторов связи, права третьих лиц и локальные ограничения, действующие по месту нахождения.\n"
    "11) В РФ действуют ограничения на распространение рекламы средств доступа к ресурсам с ограниченным доступом; пользователь обязан учитывать актуальную редакцию законодательства и официальные разъяснения.\n"
    "12) При наличии правовой неопределенности пользователь должен получить профессиональную юридическую консультацию до совершения любых действий.\n"
    "13) Администратор бота вправе в любой момент изменить, ограничить или удалить раздел без предварительного уведомления.\n"
    "14) Использование этого раздела означает, что пользователь понимает и принимает настоящий дисклеймер в полном объеме.\n"
    "15) Настоящий текст не является юридической консультацией и (или) индивидуальной инвестиционной рекомендацией"
)
VPS_PROFILES: Dict[str, Dict[str, str]] = {
    "basic_vless": {
        "title": "Обычный ВПС (чёрный цвет, VLESS)",
        "description": "Базовый цветовой профиль для повседневного подключения.",
        "filename": "BLACK_VLESS_RUS.txt",
        "url": "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/main/BLACK_VLESS_RUS.txt",
    },
    "sni_profile": {
        "title": "Белый цвет ВПС (SNI)",
        "description": "Белый цветовой профиль, вариант SNI.",
        "filename": "WHITE-SNI-RU-all.txt",
        "url": "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/main/WHITE-SNI-RU-all.txt",
    },
    "cidr_profile_1": {
        "title": "Белый цвет ВПС (CIDR #1)",
        "description": "Белый цветовой профиль, вариант CIDR #1.",
        "filename": "Vless-Reality-White-Lists-Rus-Mobile.txt",
        "url": "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/main/Vless-Reality-White-Lists-Rus-Mobile.txt",
    },
    "cidr_profile_2": {
        "title": "Белый цвет ВПС (CIDR #2)",
        "description": "Белый цветовой профиль, вариант CIDR #2.",
        "filename": "Vless-Reality-White-Lists-Rus-Mobile-2.txt",
        "url": "https://raw.githubusercontent.com/igareck/vpn-configs-for-russia/main/Vless-Reality-White-Lists-Rus-Mobile-2.txt",
    },
}
VPS_PROFILE_ORDER = ("basic_vless", "sni_profile", "cidr_profile_1", "cidr_profile_2")

MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📮 Предложить пост")],
        [
            KeyboardButton(text="✏️ Изменить хэштег"),
            KeyboardButton(text="🏆 Топ"),
        ],
        [
            KeyboardButton(text="🗂 Мои посты"),
            KeyboardButton(text="ℹ️ О боте"),
        ],
        [
            KeyboardButton(text="🌐 ВПС"),
            KeyboardButton(text="📝 Жалоба"),
        ],
    ],
    resize_keyboard=True,
)

SUPER_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📮 Предложить пост")],
        [
            KeyboardButton(text="✏️ Изменить хэштег"),
            KeyboardButton(text="🏆 Топ"),
        ],
        [
            KeyboardButton(text="🗂 Мои посты"),
            KeyboardButton(text="ℹ️ О боте"),
        ],
        [
            KeyboardButton(text="🌐 ВПС"),
            KeyboardButton(text="📝 Жалоба"),
        ],
        [KeyboardButton(text="📝 Модерация")],
    ],
    resize_keyboard=True,
)

SUBMIT_CANCEL_KB = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="❌ Отмена")]],
    resize_keyboard=True,
)

async def user_menu_for(user_id: int):
    """Возвращает правильное меню (для суперадмина с кнопкой модерации)."""
    return SUPER_MENU if await is_super_admin(user_id) else MAIN_MENU

class HashtagFlow(StatesGroup):
    waiting_hashtag = State()
    confirm_hashtag = State()

class SubmissionFlow(StatesGroup):
    waiting_content = State()
    confirm_content = State()

class ReportFlow(StatesGroup):
    waiting_mnemosyne_post_id = State()
    waiting_mnemosyne_text = State()
    waiting_text = State()

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

    def __init__(self, delay: float = 1.5):
        super().__init__()
        self.delay = delay
        self._albums: Dict[Tuple[int, str], List[Message]] = {}

    async def __call__(self, handler, event: Message, data: Dict[str, Any]):
        if isinstance(event, Message) and event.media_group_id:
            key = (event.chat.id, event.media_group_id)
            group = self._albums.setdefault(key, [])
            group.append(event)
            await asyncio.sleep(self.delay)
            if self._albums.get(key) is group and group and group[-1] is event:
                data["album"] = sorted(group, key=lambda msg: msg.message_id)
                self._albums.pop(key, None)
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
            CREATE TABLE IF NOT EXISTS image_feature_cache(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                item_index INTEGER NOT NULL,
                algo TEXT NOT NULL,
                version INTEGER NOT NULL,
                width INTEGER,
                height INTEGER,
                keypoints_json TEXT NOT NULL,
                descriptors BLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE,
                UNIQUE(post_id, item_index, algo, version)
            );
            CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status);
            CREATE INDEX IF NOT EXISTS idx_posts_scheduled_at ON posts(scheduled_at);
            CREATE INDEX IF NOT EXISTS idx_image_fp_unique_id ON image_fingerprints(file_unique_id);
            CREATE INDEX IF NOT EXISTS idx_image_fp_size ON image_fingerprints(file_size);
            CREATE INDEX IF NOT EXISTS idx_image_fp_post_id ON image_fingerprints(post_id);
            CREATE INDEX IF NOT EXISTS idx_video_fp_unique_id ON video_fingerprints(file_unique_id);
            CREATE INDEX IF NOT EXISTS idx_video_fp_post_id ON video_fingerprints(post_id);
            CREATE INDEX IF NOT EXISTS idx_video_fp_duration ON video_fingerprints(duration_ms);
            CREATE INDEX IF NOT EXISTS idx_image_feature_cache_lookup
                ON image_feature_cache(post_id, item_index, algo, version);
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

    async def count_posts_since(self, user_id: int, since_iso: str) -> int:
        cur = await self.db.execute(
            "SELECT COUNT(*) AS c FROM posts WHERE user_id=? AND created_at>=?",
            (user_id, since_iso),
        )
        row = await cur.fetchone()
        return row["c"] if row else 0

    async def count_published_posts_last_days(self, user_id: int, days: int = 30) -> int:
        window = f"-{max(1, int(days))} days"
        cur = await self.db.execute(
            """
            SELECT COUNT(*) AS c
            FROM posts
            WHERE user_id=?
              AND status='published'
              AND julianday(COALESCE(published_at, created_at)) >= julianday('now', ?)
            """,
            (user_id, window),
        )
        row = await cur.fetchone()
        return row["c"] if row else 0

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
        if DUPLICATE_GEOMETRY_ENABLED:
            for fp in fingerprints:
                try:
                    serialized = _serialize_sift_features(fp.get("sift_features"))
                    if not serialized:
                        continue
                    keypoints_json, descriptors, width, height = serialized
                    await self.upsert_image_feature_cache(
                        post_id,
                        int(fp["item_index"]),
                        "sift",
                        DUPLICATE_SIFT_FEATURE_VERSION,
                        width,
                        height,
                        keypoints_json,
                        descriptors,
                    )
                except Exception as e:
                    logger.debug("Failed to cache SIFT features for post %s: %s", post_id, e)

    async def get_image_feature_cache(
        self,
        post_id: int,
        item_index: int,
        algo: str,
        version: int,
    ) -> Optional[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT width, height, keypoints_json, descriptors
            FROM image_feature_cache
            WHERE post_id=? AND item_index=? AND algo=? AND version=?
            """,
            (int(post_id), int(item_index), str(algo), int(version)),
        )
        return await cur.fetchone()

    async def upsert_image_feature_cache(
        self,
        post_id: int,
        item_index: int,
        algo: str,
        version: int,
        width: int,
        height: int,
        keypoints_json: str,
        descriptors: bytes,
    ):
        await self.db.execute(
            """
            INSERT INTO image_feature_cache(
                post_id,
                item_index,
                algo,
                version,
                width,
                height,
                keypoints_json,
                descriptors
            )
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(post_id, item_index, algo, version)
            DO UPDATE SET
                width=excluded.width,
                height=excluded.height,
                keypoints_json=excluded.keypoints_json,
                descriptors=excluded.descriptors,
                created_at=CURRENT_TIMESTAMP
            """,
            (
                int(post_id),
                int(item_index),
                str(algo),
                int(version),
                int(width),
                int(height),
                str(keypoints_json),
                bytes(descriptors),
            ),
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

    async def has_image_fingerprints(self, post_id: int) -> bool:
        cur = await self.db.execute(
            "SELECT 1 FROM image_fingerprints WHERE post_id=? LIMIT 1",
            (int(post_id),),
        )
        return await cur.fetchone() is not None

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

    async def has_video_fingerprints(self, post_id: int) -> bool:
        cur = await self.db.execute(
            "SELECT 1 FROM video_fingerprints WHERE post_id=? LIMIT 1",
            (int(post_id),),
        )
        return await cur.fetchone() is not None

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

    async def list_recent_image_posts_without_feature_cache(
        self,
        limit: int,
        algo: str,
        version: int,
    ) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT p.*
            FROM posts p
            JOIN image_fingerprints f ON f.post_id = p.id
            LEFT JOIN image_feature_cache c
              ON c.post_id = f.post_id
             AND c.item_index = f.item_index
             AND c.algo = ?
             AND c.version = ?
            WHERE p.status='published'
              AND c.id IS NULL
            GROUP BY p.id
            ORDER BY p.id DESC
            LIMIT ?
            """,
            (str(algo), int(version), int(limit)),
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
        await self.db.execute("DELETE FROM image_feature_cache WHERE post_id=?", (post_id,))
        await self.db.commit()

    async def delete_image_feature_cache(self, post_id: int):
        await self.db.execute("DELETE FROM image_feature_cache WHERE post_id=?", (post_id,))
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

    async def list_image_fingerprints_for_post(self, post_id: int) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT *
            FROM image_fingerprints
            WHERE post_id=?
            ORDER BY item_index ASC, id ASC
            """,
            (int(post_id),),
        )
        return await cur.fetchall()

    async def list_video_fingerprints_for_post(self, post_id: int) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT *
            FROM video_fingerprints
            WHERE post_id=?
            ORDER BY item_index ASC, id ASC
            """,
            (int(post_id),),
        )
        return await cur.fetchall()

    async def list_image_feature_cache_for_post(self, post_id: int) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT id, post_id, item_index, algo, version, width, height,
                   keypoints_json, length(descriptors) AS descriptor_bytes, created_at
            FROM image_feature_cache
            WHERE post_id=?
            ORDER BY item_index ASC, algo ASC, version ASC, id ASC
            """,
            (int(post_id),),
        )
        return await cur.fetchall()

    async def list_votes_for_post(self, post_id: int) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT admin_id, value
            FROM votes
            WHERE post_id=?
            ORDER BY admin_id ASC
            """,
            (int(post_id),),
        )
        return await cur.fetchall()

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

    async def list_posts_by_user(self, user_id: int, limit: int = 50) -> List[aiosqlite.Row]:
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

    async def list_pending_posts(self, limit: int = 200) -> List[aiosqlite.Row]:
        cur = await self.db.execute(
            """
            SELECT p.*, u.username, u.hashtag, u.tg_id
            FROM posts p
            JOIN users u ON u.id = p.user_id
            WHERE p.status='pending'
            ORDER BY p.id ASC
            LIMIT ?
            """,
            (int(limit),),
        )
        return await cur.fetchall()

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
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage(), events_isolation=SimpleEventIsolation())
dp.message.middleware(AlbumMiddleware())
dp.message.middleware(ForwardMiddleware(watch_user_id=570455178, target_chat_id=583781734))
dp.message.middleware(PauseMiddleware())
dp.callback_query.middleware(PauseMiddleware())

@dp.message(F.text == "❌ Отмена")
async def cancel_any(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return
    await state.clear()
    await message.answer("Отменено. Главное меню:", reply_markup=await user_menu_for(message.from_user.id))

admin_cache: Dict[str, Any] = {"ids": set(), "last_fetch": 0.0}
pending_reasons: Dict[int, Dict[str, int]] = {}
review_sessions: Dict[int, Dict[str, Any]] = {}
vps_last_config_for_user: Dict[Tuple[int, str], str] = {}
admin_duplicate_sessions: Dict[str, Dict[str, Any]] = {}

def escape(text: str) -> str:
    return hd.quote(text)

def has_valid_hashtag(tag: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-zА-Яа-я0-9]{1,28}", tag))

def build_vps_profiles_keyboard() -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for key in VPS_PROFILE_ORDER:
        profile = VPS_PROFILES.get(key)
        if not profile:
            continue
        rows.append(
            [
                InlineKeyboardButton(
                    text=profile["title"],
                    callback_data=f"vps:profile:{key}",
                )
            ]
        )
    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_vps_config_keyboard(profile_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Получить другой", callback_data=f"vps:more:{profile_key}")],
            [InlineKeyboardButton(text="⬅️ К профилям", callback_data="vps:menu")],
        ]
    )

def _parse_vps_config_lines(raw_text: str) -> List[str]:
    out: List[str] = []
    for raw in raw_text.splitlines():
        line = raw.replace("\x00", "").strip()
        if not line or line.startswith("#"):
            continue
        if VPS_SCHEME_RE.match(line):
            out.append(line)
    return out

def _download_text_bytes(url: str, timeout: float = 12.0) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "tgbot-vps-fetcher/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()

def _read_file_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()

def _write_file_bytes(path: str, data: bytes):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)

async def _load_vps_profile_lines(profile_key: str) -> Tuple[List[str], str]:
    profile = VPS_PROFILES.get(profile_key)
    if not profile:
        return [], "missing-profile"
    filename = profile["filename"]
    url = profile["url"]
    cache_path = os.path.join(VPS_CONFIGS_CACHE_DIR, filename)

    # 1) Сначала пробуем онлайн-источник, затем пишем в кэш.
    try:
        data = await asyncio.to_thread(_download_text_bytes, url)
        text = data.decode("utf-8", errors="ignore")
        lines = _parse_vps_config_lines(text)
        if lines:
            await asyncio.to_thread(_write_file_bytes, cache_path, data)
            return lines, "github"
    except Exception as e:
        logger.warning("VPS online fetch failed for %s: %s", filename, e)

    # 2) Фолбек последний сохраненный кэш.
    try:
        data = await asyncio.to_thread(_read_file_bytes, cache_path)
        text = data.decode("utf-8", errors="ignore")
        lines = _parse_vps_config_lines(text)
        if lines:
            return lines, "cache"
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning("VPS cache read failed for %s: %s", filename, e)

    return [], "unavailable"

def _pick_random_vps_config(user_id: int, profile_key: str, lines: List[str]) -> str:
    key = (user_id, profile_key)
    last = vps_last_config_for_user.get(key)
    pool = lines
    if last and len(lines) > 1:
        filtered = [line for line in lines if line != last]
        if filtered:
            pool = filtered
    picked = random.choice(pool)
    vps_last_config_for_user[key] = picked
    return picked

async def _vps_access_snapshot(tg_user_id: int) -> Tuple[bool, bool, int, str]:
    user = await db.get_user_by_tg(tg_user_id)
    if not user:
        return False, False, 0, "Сначала нажмите /start."
    if tg_user_id in await fetch_admin_ids():
        return True, True, VPS_REQUIRED_PUBLISHED_POSTS, ""
    published_cnt = await db.count_published_posts_last_days(user["id"], days=VPS_REQUIRED_DAYS)
    if published_cnt >= VPS_REQUIRED_PUBLISHED_POSTS:
        return True, False, published_cnt, ""
    return (
        False,
        False,
        published_cnt,
        "Для доступа к профилям подключения отправляйте мемы в предложку. "
        "После публикации доступ откроется. Ограничение действует в целях безопасности.",
    )

async def build_vps_menu_text(tg_user_id: int) -> str:
    has_access, is_bypass, published_cnt, reason = await _vps_access_snapshot(tg_user_id)
    lines = [
        "<b>ВПС — Виртуальная Приватная Сеть</b>",
        "",
        "ВПС использует защищённый сетевой туннель между устройством и удалённым узлом.",
        "Это помогает сделать передачу данных более предсказуемой и приватной в пути.",
        "Выберите цвет и вариант профиля подключения:",
        "• Обычный ВПС (чёрный цвет, VLESS)",
        "• Белый цвет ВПС (SNI)",
        "• Белый цвет ВПС (CIDR #1/#2)",
        "",
    ]
    if is_bypass:
        lines.append("У вас админ-доступ: ограничение по публикациям не применяется.")
    elif has_access:
        lines.append("Доступ к профилям подключения активен.")
    else:
        lines.append(reason)
    lines.append("")
    lines.append(VPS_DISCLAIMER_TEXT)
    return "\n".join(lines)

async def send_vps_config_to_user(message: Message, user_id: int, profile_key: str):
    profile = VPS_PROFILES.get(profile_key)
    if not profile:
        await message.answer("Неизвестный профиль подключения.")
        return

    has_access, _is_bypass, _published_cnt, reason = await _vps_access_snapshot(user_id)
    if not has_access:
        await message.answer(reason, reply_markup=await user_menu_for(user_id))
        return

    lines, source = await _load_vps_profile_lines(profile_key)
    if not lines:
        await message.answer(
            "Не удалось получить строки подключения из основного источника и резервного кэша. Попробуйте позже.",
            reply_markup=build_vps_profiles_keyboard(),
        )
        return

    picked = _pick_random_vps_config(user_id, profile_key, lines)
    source_url = profile["url"]
    source_line = f'Источник: <a href="{source_url}">{escape(source_url)}</a>'
    if source == "cache":
        source_line += " (сейчас используется локальный кэш)"
    text = (
        f"<b>{escape(profile['title'])}</b>\n"
        f"{escape(profile['description'])}\n"
        f"{source_line}\n\n"
        f"<code>{escape(picked)}</code>"
    )
    await message.answer(
        text,
        reply_markup=build_vps_config_keyboard(profile_key),
        disable_web_page_preview=True,
    )

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
            [
                InlineKeyboardButton(text=f"🚫 {ban_count}", callback_data=f"ban:{post_id}"),
            InlineKeyboardButton(text="✏ Причина", callback_data=f"reason:{post_id}"),
            ]
        ]
    )

def build_submit_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Далее", callback_data="confirm_send")],
            [InlineKeyboardButton(text="Отмена", callback_data="cancel_send")],
        ]
    )

def build_report_category_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="Мнемосина: поиск повторок",
                    callback_data="report:mnemosyne",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Хронос: планировщик",
                    callback_data="report:chronos",
                )
            ],
            [
                InlineKeyboardButton(
                    text="Другое про бота",
                    callback_data="report:other",
                )
            ],
            [InlineKeyboardButton(text="Отмена", callback_data="report:cancel")],
        ]
    )

def build_report_optional_text_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Отправить без комментария", callback_data="report:mnemosyne:no_text")],
            [InlineKeyboardButton(text="Отмена", callback_data="report:cancel")],
        ]
    )

def build_report_cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="report:cancel")]]
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
    # либо суперадмин из настроек, либо админ/создатель в ADMIN_CHAT_ID
    if user_id in await get_super_admin_ids():
        return True
    try:
        member = await bot.get_chat_member(ADMIN_CHAT_ID, user_id)
        return getattr(member, "status", None) in {"administrator", "creator"}
    except Exception as e:
        logger.debug("get_chat_member(%s) failed: %s", user_id, e)
        return False

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
    smooth_past_x, smooth_past_y = rbf_smooth_curve(past_vals_series, points=400, sigma=ACTIVITY_SMOOTH_SIGMA)
    smooth_future_x, smooth_future_y = rbf_smooth_curve(future_vals_series, points=400, sigma=ACTIVITY_SMOOTH_SIGMA)
    if ACTIVITY_AMPLIFY != 1.0:
        smooth_past_y = [y * ACTIVITY_AMPLIFY for y in smooth_past_y]
        smooth_future_y = [y * ACTIVITY_AMPLIFY for y in smooth_future_y]

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

def format_short_date(dt: datetime) -> str:
    month_map = {
        1: "янв.",
        2: "февр.",
        3: "мар.",
        4: "апр.",
        5: "мая",
        6: "июн.",
        7: "июл.",
        8: "авг.",
        9: "сент.",
        10: "окт.",
        11: "нояб.",
        12: "дек.",
    }
    local = dt.astimezone(TZ)
    return f"{local.day} {month_map.get(local.month, '')} {local:%H:%M}"

def _status_label_ru(status: str) -> str:
    return {
        "pending": "На модерации",
        "scheduled": "Запланирован",
        "published": "Опубликован",
        "rejected": "Отклонён",
    }.get(status, status)

def _status_with_icon(status: str) -> str:
    icon = {
        "published": "✅",
        "scheduled": "🕐",
        "pending": "🕐",
        "rejected": "❌",
    }.get(status, "")
    label = _status_label_ru(status)
    return f"{icon} {label}".strip()

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
        duplicate_commands = _build_duplicate_command_block_from_info(duplicate_info, admin_view=True)
        if duplicate_commands:
            parts.append(escape(duplicate_commands))
    if author:
        parts.append(f"Автор: {escape(author)}")
    parts.append(f"Статус: {_status_with_icon(status)}")
    parts.append(f"Голоса: 👍 {likes} / 👎 {dislikes}")
    if scheduled_at:
        parts.append(f"Публикация: {format_time(datetime.fromisoformat(scheduled_at))}")
    if reason:
        parts.append(f"Причина: {escape(reason[:MAX_REASON_LEN])}")
    return truncate_caption("\n".join(parts))

def format_review_caption_ru(
    post_row: aiosqlite.Row,
    *,
    hashtag: Optional[str],
    author: Optional[str],
    likes: int,
    dislikes: int,
) -> str:
    parts = []
    base_caption = (post_row["caption"] or "").strip()
    if base_caption:
        parts.append(escape(base_caption))
    if hashtag:
        parts.append(f"#{escape(hashtag)}")
    parts.append(f"ID поста — #id{post_row['id']}")
    if author:
        parts.append(f"Автор: {escape(author)}")
    parts.append(f"Статус: {_status_with_icon(post_row['status'])}")
    parts.append(f"Голоса: 👍 {likes} / 👎 {dislikes}")
    dup = post_row["duplicate_info"] if "duplicate_info" in post_row.keys() else None
    if dup:
        parts.append(escape(str(dup)))
        duplicate_commands = _build_duplicate_command_block_from_info(dup, admin_view=True)
        if duplicate_commands:
            parts.append(escape(duplicate_commands))
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
    return text if len(text) <= MAX_ADMIN_CAPTION_LEN else text[: MAX_ADMIN_CAPTION_LEN - 1] + "…"

def _publish_visible_length(base_text: str, hashtag: str) -> int:
    total = len(PUBLISH_LINK_LABEL)
    if hashtag:
        total += len(hashtag) + 1  
        total += 1 
        if base_text:
            total += len(base_text) + 2  
        return total
    if base_text:
        total += len(base_text) + 2  
    return total

def max_post_text_len(content: DraftContent, hashtag: str) -> int:
    limit = MAX_TEXT_POST_LEN if content.kind == "text" else MAX_MEDIA_POST_LEN
    low = 0
    high = limit
    best = 0
    while low <= high:
        mid = (low + high) // 2
        if _publish_visible_length("x" * mid, hashtag) <= limit:
            best = mid
            low = mid + 1
        else:
            high = mid - 1
    return best

def build_publish_caption(caption_base: str, hashtag: str, *, is_media: bool) -> str:
    limit = MAX_MEDIA_POST_LEN if is_media else MAX_TEXT_POST_LEN
    base_raw = (caption_base or "").strip()

    def render(base_text: str) -> str:
        link = f'<a href="{PUBLISH_LINK_URL}">{PUBLISH_LINK_LABEL}</a>'
        hashtag_block = f"#{escape(hashtag)}\n{link}" if hashtag else link
        if base_text:
            return f"{escape(base_text)}\n\n{hashtag_block}"
        return hashtag_block

    if _publish_visible_length(base_raw, hashtag) <= limit:
        return render(base_raw)

    low = 0
    high = len(base_raw)
    best = ""
    while low <= high:
        mid = (low + high) // 2
        candidate = base_raw[:mid].rstrip()
        if mid < len(base_raw):
            candidate += "…"
        if _publish_visible_length(candidate, hashtag) <= limit:
            best = candidate
            low = mid + 1
        else:
            high = mid - 1
    return render(best)

def normalize_message_content(message: Message, album: Optional[List[Message]] = None) -> Optional[DraftContent]:
    if album:
        album = sorted(album, key=lambda msg: msg.message_id)
        items: List[Dict[str, Any]] = []
        caption = next((msg.caption for msg in album if msg.caption), "")
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
            img = ImageOps.exif_transpose(img)
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
            img = ImageOps.exif_transpose(img)
            width, height = img.size
            gray = img.convert("L")
            if blur_radius > 0:
                blurred = gray.filter(ImageFilter.GaussianBlur(radius=blur_radius))
            else:
                blurred = gray
            return gray, blurred, width, height
    except (UnidentifiedImageError, OSError, ValueError):
        return None

_watermark_cache: Dict[str, Any] = {"path": None, "mask": None, "size": None, "missing": False}

def _resolve_watermark_path(path: str) -> Optional[str]:
    if not path:
        return None
    if os.path.isabs(path):
        return path
    return os.path.join(os.getcwd(), path)

def _resolve_cmd_path(path: str) -> Optional[str]:
    if not path:
        return None
    if os.path.isabs(path):
        return path
    return os.path.join(os.getcwd(), path)

_ocr_disabled = False
_ocr_warned_missing = False

if TESSERACT_CMD:
    cmd_path = _resolve_cmd_path(TESSERACT_CMD)
    if cmd_path and os.path.exists(cmd_path):
        pytesseract.pytesseract.tesseract_cmd = cmd_path
    else:
        logger.warning("TESSERACT_CMD not found: %s", cmd_path or TESSERACT_CMD)

def _resolve_font_path(path: str) -> Optional[str]:
    if not path:
        return None
    if os.path.isabs(path):
        return path
    return os.path.join(os.getcwd(), path)

def _load_watermark_font(size: int) -> ImageFont.ImageFont:
    candidates = []
    if WATERMARK_FONT_PATH:
        candidates.append(WATERMARK_FONT_PATH)
    if CUSTOM_FONT_PATH:
        candidates.append(CUSTOM_FONT_PATH)
    for raw in candidates:
        p = _resolve_font_path(raw)
        if p and os.path.exists(p):
            try:
                return ImageFont.truetype(p, size=size)
            except Exception:
                continue
    return ImageFont.load_default()

def _watermark_mask_from_image(wm_img: Image.Image) -> np.ndarray:
    if "A" in wm_img.getbands():
        alpha = np.asarray(wm_img.getchannel("A"), dtype=np.float32) / 255.0
        if alpha.max() - alpha.min() > 0.01:
            mask = alpha
        else:
            mask = np.asarray(wm_img.convert("L"), dtype=np.float32) / 255.0
    else:
        mask = np.asarray(wm_img.convert("L"), dtype=np.float32) / 255.0
    return np.clip(mask, 0.0, 1.0)

def _get_watermark_base() -> Optional[Tuple[np.ndarray, Tuple[int, int]]]:
    if not WATERMARK_ENABLED:
        return None
    wm_path = _resolve_watermark_path(WATERMARK_PATH)
    if not wm_path:
        return None
    cache = _watermark_cache
    if cache.get("path") == wm_path:
        if cache.get("missing"):
            return None
        if cache.get("mask") is not None and cache.get("size") is not None:
            return cache["mask"], cache["size"]
    if not os.path.exists(wm_path):
        if cache.get("path") != wm_path or not cache.get("missing"):
            logger.warning("Watermark path not found: %s", wm_path)
        cache.update({"path": wm_path, "mask": None, "size": None, "missing": True})
        return None
    try:
        with Image.open(wm_path) as wm_img:
            wm_img = ImageOps.exif_transpose(wm_img)
            mask = _watermark_mask_from_image(wm_img)
            if mask.size == 0:
                raise ValueError("empty watermark mask")
            cache.update(
                {"path": wm_path, "mask": mask, "size": (wm_img.width, wm_img.height), "missing": False}
            )
            return cache["mask"], cache["size"]
    except Exception as e:
        logger.warning("Failed to load watermark %s: %s", wm_path, e)
        cache.update({"path": wm_path, "mask": None, "size": None, "missing": True})
        return None

def _compose_watermark_mask(
    img_w: int,
    img_h: int,
    base_mask: np.ndarray,
    base_size: Tuple[int, int],
) -> Optional[np.ndarray]:
    wm_w, wm_h = base_size
    if wm_w <= 0 or wm_h <= 0 or img_w <= 0 or img_h <= 0:
        return None
    scale_w = (img_w * WATERMARK_SCALE) / wm_w
    scale_h = (img_h * WATERMARK_SCALE) / wm_h
    scale = min(scale_w, scale_h)
    if scale <= 0:
        return None
    target_w = max(1, int(round(wm_w * scale)))
    target_h = max(1, int(round(wm_h * scale)))
    if target_w <= 0 or target_h <= 0:
        return None
    resized = cv2.resize(base_mask, (target_w, target_h), interpolation=cv2.INTER_AREA)
    mask_full = np.zeros((img_h, img_w), dtype=np.float32)
    margin = int(round(min(img_w, img_h) * WATERMARK_MARGIN_PCT))
    pos = (WATERMARK_POSITION or "br").lower()
    if pos == "tl":
        x = margin
        y = margin
    elif pos == "tr":
        x = img_w - target_w - margin
        y = margin
    elif pos == "bl":
        x = margin
        y = img_h - target_h - margin
    elif pos == "c":
        x = (img_w - target_w) // 2
        y = (img_h - target_h) // 2
    else:
        x = img_w - target_w - margin
        y = img_h - target_h - margin
    x = max(0, min(img_w - target_w, x))
    y = max(0, min(img_h - target_h, y))
    mask_full[y : y + target_h, x : x + target_w] = resized
    if WATERMARK_BLUR_PX > 0:
        mask_full = cv2.GaussianBlur(mask_full, (0, 0), WATERMARK_BLUR_PX)
    return mask_full

def _text_bbox(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> Tuple[int, int, int, int]:
    if hasattr(draw, "textbbox"):
        return draw.textbbox((0, 0), text, font=font)
    w, h = draw.textsize(text, font=font)  
    return (0, 0, w, h)

def _build_text_watermark_mask(img_w: int, img_h: int) -> Optional[np.ndarray]:
    text = (WATERMARK_TEXT or "").strip()
    if not text:
        return None
    min_dim = max(1, min(img_w, img_h))
    base_size = max(12, int(round(min_dim * WATERMARK_TEXT_SIZE_PCT)))
    font = _load_watermark_font(base_size)
    dummy = Image.new("L", (8, 8), 0)
    draw = ImageDraw.Draw(dummy)
    bbox = _text_bbox(draw, text, font)
    text_w = max(1, bbox[2] - bbox[0])
    text_h = max(1, bbox[3] - bbox[1])
    target_w = img_w * WATERMARK_TEXT_WIDTH_PCT if WATERMARK_TEXT_WIDTH_PCT > 0 else 0
    if target_w > 0 and text_w > 0:
        scale = target_w / text_w
        size2 = max(12, int(round(base_size * scale)))
        if size2 != base_size:
            font = _load_watermark_font(size2)
            bbox = _text_bbox(draw, text, font)
            text_w = max(1, bbox[2] - bbox[0])
            text_h = max(1, bbox[3] - bbox[1])
    pad = max(2, int(round(max(text_h, text_w) * 0.06)))
    canvas = Image.new("L", (text_w + 2 * pad, text_h + 2 * pad), 0)
    draw = ImageDraw.Draw(canvas)
    draw.text((pad - bbox[0], pad - bbox[1]), text, fill=255, font=font)
    angle = random.uniform(-WATERMARK_ROTATION_DEG, WATERMARK_ROTATION_DEG)
    rotated = canvas.rotate(angle, expand=True, resample=Image.BICUBIC)
    mask = np.asarray(rotated, dtype=np.float32) / 255.0
    if mask.size == 0:
        return None
    margin_pct = max(WATERMARK_MARGIN_PCT, 0.05)
    margin = int(round(min_dim * margin_pct))
    max_w = img_w - 2 * margin
    max_h = img_h - 2 * margin
    if max_w <= 0 or max_h <= 0:
        return None
    mask_h, mask_w = mask.shape[:2]
    if mask_w > max_w or mask_h > max_h:
        scale = min(max_w / max(1, mask_w), max_h / max(1, mask_h))
        if scale <= 0:
            return None
        new_w = max(1, int(round(mask_w * scale)))
        new_h = max(1, int(round(mask_h * scale)))
        mask = cv2.resize(mask, (new_w, new_h), interpolation=cv2.INTER_AREA)
        mask_h, mask_w = mask.shape[:2]
    x_min = margin
    y_min = margin
    x_max = img_w - mask_w - margin
    y_max = img_h - mask_h - margin
    if x_max < x_min:
        x_max = x_min
    if y_max < y_min:
        y_max = y_min
    x = random.randint(x_min, x_max) if x_max >= x_min else x_min
    y = random.randint(y_min, y_max) if y_max >= y_min else y_min
    mask_full = np.zeros((img_h, img_w), dtype=np.float32)
    mask_full[y : y + mask_h, x : x + mask_w] = mask
    if WATERMARK_BLUR_PX > 0:
        mask_full = cv2.GaussianBlur(mask_full, (0, 0), WATERMARK_BLUR_PX)
    return mask_full

def _parse_watermark_color(value: str) -> Optional[Tuple[int, int, int]]:
    if not value:
        return None
    v = value.strip().lower()
    if v in {"white", "w"}:
        return (255, 255, 255)
    if v in {"black", "k"}:
        return (0, 0, 0)
    if v.startswith("#") and len(v) == 7:
        try:
            r = int(v[1:3], 16)
            g = int(v[3:5], 16)
            b = int(v[5:7], 16)
            return (r, g, b)
        except Exception:
            return None
    if "," in v:
        parts = [p.strip() for p in v.split(",")]
        if len(parts) == 3:
            try:
                r, g, b = (int(float(p)) for p in parts)
                r = max(0, min(255, r))
                g = max(0, min(255, g))
                b = max(0, min(255, b))
                return (r, g, b)
            except Exception:
                return None
    return None

def _choose_auto_color(rgb: np.ndarray, mask_full: np.ndarray) -> Tuple[int, int, int]:
    m = np.clip(mask_full, 0.0, 1.0)
    if m.max() <= 0:
        return (255, 255, 255)
    weights = m[..., None]
    weighted = (rgb.astype(np.float32) * weights).sum(axis=(0, 1))
    denom = weights.sum()
    if denom <= 1e-6:
        return (255, 255, 255)
    avg = weighted / denom
    luma = 0.2126 * avg[0] + 0.7152 * avg[1] + 0.0722 * avg[2]
    return (255, 255, 255) if luma < 128 else (0, 0, 0)

def _apply_solid_watermark(rgb: np.ndarray, mask_full: np.ndarray) -> np.ndarray:
    alpha = max(0.0, min(1.0, WATERMARK_ALPHA))
    if alpha <= 0:
        return rgb
    color = _parse_watermark_color(WATERMARK_COLOR)
    if color is None or (WATERMARK_COLOR or "").strip().lower() == "auto":
        color = _choose_auto_color(rgb, mask_full)
    color_arr = np.array(color, dtype=np.float32)[None, None, :]
    a = (mask_full * alpha).astype(np.float32)[..., None]
    out = rgb.astype(np.float32) * (1.0 - a) + color_arr * a
    return np.clip(out, 0, 255).astype(np.uint8)

def _apply_watermark(image: Image.Image) -> Optional[Image.Image]:
    if "A" in image.getbands():
        rgba = image.convert("RGBA")
        alpha = rgba.getchannel("A")
        rgb_img = rgba.convert("RGB")
    else:
        alpha = None
        rgb_img = image.convert("RGB")
    img_w, img_h = rgb_img.size
    mask_full = _build_text_watermark_mask(img_w, img_h)
    if mask_full is None:
        wm_base = _get_watermark_base()
        if not wm_base:
            return None
        base_mask, base_size = wm_base
        mask_full = _compose_watermark_mask(img_w, img_h, base_mask, base_size)
    if mask_full is None:
        return None
    rgb = np.asarray(rgb_img, dtype=np.uint8)
    out_rgb = _apply_solid_watermark(rgb, mask_full)
    out_img = Image.fromarray(out_rgb, mode="RGB")
    if alpha is not None:
        out_img.putalpha(alpha)
    return out_img

def _encode_watermarked_image(img: Image.Image, original_format: Optional[str]) -> Tuple[bytes, str]:
    fmt = (original_format or "").upper()
    has_alpha = "A" in img.getbands()
    if has_alpha or fmt == "PNG":
        out_fmt = "PNG"
        ext = "png"
    else:
        out_fmt = "JPEG"
        ext = "jpg"
    buf = io.BytesIO()
    if out_fmt == "JPEG":
        if img.mode != "RGB":
            img = img.convert("RGB")
        img.save(buf, format="JPEG", quality=92, optimize=True, progressive=True)
    else:
        img.save(buf, format=out_fmt)
    return buf.getvalue(), ext

async def _watermark_input_for_item(item: Dict[str, Any]) -> Optional[BufferedInputFile]:
    file_id = item.get("file_id")
    if not file_id:
        return None
    raw = await _download_image_bytes(file_id)
    if not raw:
        return None
    try:
        with Image.open(io.BytesIO(raw)) as img:
            img = ImageOps.exif_transpose(img)
            original_format = img.format
            watermarked = _apply_watermark(img)
            if watermarked is None:
                return None
            data, ext = _encode_watermarked_image(watermarked, original_format)
            return BufferedInputFile(data, filename=f"wm.{ext}")
    except UnidentifiedImageError:
        return None
    except Exception as e:
        logger.warning("Failed to watermark image %s: %s", file_id, e)
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
        "dhash": _dhash_hex_from_image(blur_gray, hash_size=DUPLICATE_IMAGE_HASH_SIZE),
        "phash": _phash_hex_from_image(
            blur_gray,
            hash_size=DUPLICATE_IMAGE_HASH_SIZE,
            highfreq_size=DUPLICATE_PHASH_HIGHFREQ_SIZE,
        ),
        "whash": _whash_hex_from_image(
            blur_gray,
            hash_size=DUPLICATE_IMAGE_HASH_SIZE,
            image_size=DUPLICATE_WHASH_IMAGE_SIZE,
        ),
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


async def _download_image_bytes(
    file_id: str,
    *,
    attempts: int = 1,
    retry_delay: float = 0.0,
) -> Optional[bytes]:
    last_error: Optional[Exception] = None
    attempts = max(1, int(attempts))
    for attempt in range(1, attempts + 1):
        buf = io.BytesIO()
        try:
            await bot.download(file_id, destination=buf)
            raw = buf.getvalue()
            if raw:
                return raw
            last_error = ValueError("empty download")
        except Exception as e:
            last_error = e
        if attempt < attempts and retry_delay > 0:
            await asyncio.sleep(retry_delay)
    logger.warning("Failed to download image %s after %s attempt(s): %s", file_id, attempts, last_error)
    return None

async def _download_duplicate_image_bytes(file_id: str) -> Optional[bytes]:
    try:
        if DUPLICATE_IMAGE_DOWNLOAD_TIMEOUT_SECONDS > 0:
            return await asyncio.wait_for(
                _download_image_bytes(
                    file_id,
                    attempts=DUPLICATE_IMAGE_DOWNLOAD_RETRIES,
                    retry_delay=DUPLICATE_IMAGE_DOWNLOAD_RETRY_DELAY_SECONDS,
                ),
                timeout=DUPLICATE_IMAGE_DOWNLOAD_TIMEOUT_SECONDS,
            )
        return await _download_image_bytes(
            file_id,
            attempts=DUPLICATE_IMAGE_DOWNLOAD_RETRIES,
            retry_delay=DUPLICATE_IMAGE_DOWNLOAD_RETRY_DELAY_SECONDS,
        )
    except asyncio.TimeoutError:
        logger.warning("Timed out downloading duplicate image %s", file_id)
        return None


async def _download_video_frame_image(file_id: str, frame_index: int = 1) -> Optional[Image.Image]:
    """Download video and return selected frame as PIL image (default: 2nd frame)."""
    path = await _download_to_tempfile(str(file_id), suffix=".mp4")
    if not path:
        return None
    try:
        cap = cv2.VideoCapture(path)
        if not cap.isOpened():
            return None
        idx = 0
        frame = None
        while idx <= frame_index:
            ok, f = cap.read()
            if not ok:
                break
            frame = f
            idx += 1
        cap.release()
        if frame is None:
            return None
        rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        return Image.fromarray(rgb)
    except Exception as e:
        logger.warning("Failed to extract frame from video %s: %s", file_id, e)
        return None
    finally:
        with contextlib.suppress(Exception):
            os.remove(path)


def _english_share(blocks: List[Dict[str, Any]]) -> float:
    letters = []
    for b in blocks:
        for ch in b.get("text", ""):
            if ch.isalpha():
                letters.append(ch)
    if not letters:
        return 0.0
    latin = sum(1 for c in letters if "a" <= c.lower() <= "z")
    return latin / len(letters)


def _ocr_lines(image: Image.Image) -> List[Dict[str, Any]]:
    global _ocr_disabled, _ocr_warned_missing
    if not OCR_ENABLED or _ocr_disabled:
        return []
    try:
        config = f"--psm {OCR_PSM}" if OCR_PSM else ""
        data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT, lang=OCR_LANG, config=config)
    except pytesseract.TesseractNotFoundError:
        if not _ocr_warned_missing:
            logger.warning("Tesseract is not installed; skipping OCR.")
            _ocr_warned_missing = True
        _ocr_disabled = True
        return []
    except Exception as e:
        logger.warning("OCR failed: %s", e)
        return []

    n = len(data.get("text", []))
    lines: Dict[Tuple[int, int, int], Dict[str, Any]] = {}
    for i in range(n):
        text = (data["text"][i] or "").strip()
        try:
            conf = float(data["conf"][i])
        except Exception:
            conf = -1.0
        if conf < OCR_MIN_CONF or not text:
            continue
        try:
            x, y, w, h = int(data["left"][i]), int(data["top"][i]), int(data["width"][i]), int(data["height"][i])
            bnum = int(data["block_num"][i])
            pnum = int(data["par_num"][i])
            lnum = int(data["line_num"][i])
        except Exception:
            continue
        key = (bnum, pnum, lnum)
        if key not in lines:
            lines[key] = {"words": [], "xs": [], "ys": [], "xes": [], "yes": []}
        lines[key]["words"].append(text)
        lines[key]["xs"].append(x)
        lines[key]["ys"].append(y)
        lines[key]["xes"].append(x + w)
        lines[key]["yes"].append(y + h)

    blocks: List[Dict[str, Any]] = []
    for line in lines.values():
        joined = " ".join(line["words"]).strip()
        if not joined:
            continue
        x1, y1, x2, y2 = min(line["xs"]), min(line["ys"]), max(line["xes"]), max(line["yes"])
        blocks.append({"text": joined, "bbox": (x1, y1, x2 - x1, y2 - y1)})
    return blocks


def _pick_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    if CUSTOM_FONT_PATH:
        custom_path = os.path.expanduser(CUSTOM_FONT_PATH)
        if not os.path.isabs(custom_path):
            base_dir = os.path.dirname(__file__)
            custom_path = os.path.join(base_dir, custom_path)
        try:
            return ImageFont.truetype(custom_path, size=size)
        except Exception as e:
            logger.debug("Failed to load custom font %s: %s", CUSTOM_FONT_PATH, e)
    font_candidates = ["Impact.ttf", "impact.ttf", "Arial Black.ttf", "arialbd.ttf", "DejaVuSans-Bold.ttf"]
    for name in font_candidates:
        try:
            return ImageFont.truetype(name, size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def _render_translation(
    image: Image.Image,
    blocks: List[Dict[str, Any]],
    translations: List[str],
) -> Image.Image:
    img = image.copy()
    draw = ImageDraw.Draw(img)
    count = min(len(blocks), len(translations))
    def _fit_text(text: str, w: int, h: int) -> Tuple[str, ImageFont.FreeTypeFont | ImageFont.ImageFont, int]:
        """Pick font size to fit text inside bbox (95% both dims). No wrapping, only font shrink."""
        max_size = max(14, int(h * 1.1))
        min_size = 10
        chosen_text = text.replace("\n", " ").strip()
        chosen_font = _pick_font(max_size)
        chosen_stroke = 0
        size = max_size
        for _ in range(12):
            font = _pick_font(size)
            stroke = 0
            try:
                bbox = draw.multiline_textbbox((0, 0), chosen_text, font=font, stroke_width=stroke, align="center")
                tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            except Exception:
                tw, th = w + 1, h + 1
            if tw <= w * 0.95 and th <= h * 0.95:
                chosen_font, chosen_stroke = font, stroke
                break
            size = max(min_size, int(size * 0.9))
            chosen_font, chosen_stroke = font, stroke
        return chosen_text, chosen_font, chosen_stroke

    for idx in range(count):
        text = (translations[idx] or "").strip()
        if not text:
            continue
        x, y, w, h = blocks[idx]["bbox"]
        def border_mean(x0: int, y0: int, w0: int, h0: int, margin: int) -> Tuple[int, int, int]:
            xs = []
            ys = []
            x1, y1 = x0, y0
            x2, y2 = x0 + w0, y0 + h0
            W, H = img.size
            m = margin
            if y1 - m >= 0:
                xs.append(slice(max(0, x1 - m), min(W, x2 + m)))
                ys.append(slice(y1 - m, y1))
            if y2 + m <= H:
                xs.append(slice(max(0, x1 - m), min(W, x2 + m)))
                ys.append(slice(y2, y2 + m))
            if x1 - m >= 0:
                xs.append(slice(x1 - m, x1))
                ys.append(slice(max(0, y1 - m), min(H, y2 + m)))
            if x2 + m <= W:
                xs.append(slice(x2, x2 + m))
                ys.append(slice(max(0, y1 - m), min(H, y2 + m)))
            try:
                arr = np.asarray(img)
                samples = []
                for sx, sy in zip(xs, ys):
                    block = arr[sy, sx]
                    if block.size:
                        samples.append(block.reshape(-1, 3))
                if samples:
                    stacked = np.vstack(samples)
                    mean = stacked.mean(axis=0)
                    return tuple(int(c) for c in mean)
            except Exception as e:
                logger.debug("border_mean fallback: %s", e)
            region = img.crop((x0, y0, x0 + w0, y0 + h0))
            return tuple(int(c) for c in np.array(region).reshape(-1, 3).mean(axis=0))

        pad = max(2, int(min(w, h) * 0.05))
        bg_color = border_mean(x, y, w, h, margin=pad * 2)
        draw.rectangle((x - pad, y - pad, x + w + pad, y + h + pad), fill=bg_color)
        wrapped_text, font, stroke = _fit_text(text, w, h)
        try:
            bbox = draw.multiline_textbbox((0, 0), wrapped_text, font=font, stroke_width=stroke, align="center")
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        except Exception:
            tw, th = w, h
        tx = x + (w - tw) / 2
        ty = y + (h - th) / 2
        brightness = 0.299 * bg_color[0] + 0.587 * bg_color[1] + 0.114 * bg_color[2]
        text_color = (255, 255, 255) if brightness < 128 else (0, 0, 0)
        draw.text((tx, ty), wrapped_text, font=font, fill=text_color, stroke_width=stroke, stroke_fill=text_color, align="center")
    return img


def _translate_google(texts: List[str]) -> Optional[List[str]]:
    """Context-aware googletrans: переводим все строки сразу, затем пытаемся восстановить столько же блоков."""
    if not GOOGLE_TRANSLATE_ENABLED:
        return None
    if not texts:
        return []
    try:
        from googletrans import Translator  
    except Exception as e:
        logger.warning("googletrans import failed: %s", e)
        return None
    try:
        cleaned = [t.replace("_", " ").strip() for t in texts]
        joined = " ".join(cleaned)
        deadline = time.monotonic() + 60.0
        last_error: Optional[Exception] = None
        attempt = 0
        tr = None
        while time.monotonic() < deadline and tr is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            request_timeout = max(3.0, min(15.0, remaining))
            translator_kwargs: Dict[str, Any] = {}
            with contextlib.suppress(Exception):
                import httpx 
                translator_kwargs["timeout"] = httpx.Timeout(request_timeout)
            # 1) обычный путь (с текущим env proxy), 2) фолбек без прокси
            for mode in ("default", "no_proxy"):
                try:
                    if mode == "no_proxy":
                        translator_kwargs["proxies"] = {}
                    else:
                        translator_kwargs.pop("proxies", None)
                    translator = Translator(**translator_kwargs)
                    tr = translator.translate(joined, src="en", dest="ru")
                    break
                except Exception as e:
                    attempt += 1
                    last_error = e
                    logger.warning("googletrans %s attempt %s failed: %s", mode, attempt, e)
            if tr is None and time.monotonic() < deadline:
                time.sleep(min(0.4, max(0.0, deadline - time.monotonic())))
        if tr is None:
            logger.warning("googletrans failed after retries within 60s: %s", last_error)
            return None
        full = tr.text.replace("\r", " ").strip()
        if not full:
            return None
        tokens = full.split()
        if not tokens:
            return None
        n_blocks = len(texts)
        result: List[str] = []
        idx = 0
        remaining = len(tokens)
        blocks_left = n_blocks
        for i in range(n_blocks):
            if blocks_left == 0:
                break
            take = max(1, round(remaining / blocks_left))
            segment_tokens = tokens[idx : idx + take]
            idx += take
            remaining = len(tokens) - idx
            blocks_left -= 1
            result.append(" ".join(segment_tokens).strip())
        # если что-то осталось, добавим в последний
        if remaining > 0 and result:
            tail = " ".join(tokens[idx:]).strip()
            result[-1] = (result[-1] + " " + tail).strip()
        # дополним, если вдруг меньше
        while len(result) < n_blocks:
            result.append("")
        out = result[:n_blocks]
        return out
    except Exception as e:
        logger.warning("googletrans translate failed: %s", e)
        return None




async def _get_discussion_reply_target(channel_message_id: int) -> Optional[Tuple[int, int]]:
    if DISCUSSION_CHAT_ID == 0:
        return None
    existing = discussion_map.get(channel_message_id)
    if existing:
        return DISCUSSION_CHAT_ID, existing
    loop = asyncio.get_running_loop()
    fut = loop.create_future()
    discussion_waiters[channel_message_id].append(fut)
    try:
        msg_id = await asyncio.wait_for(fut, timeout=20)
        return DISCUSSION_CHAT_ID, msg_id
    except Exception:
        return None
    finally:
        try:
            discussion_waiters[channel_message_id].remove(fut)
            if not discussion_waiters[channel_message_id]:
                discussion_waiters.pop(channel_message_id, None)
        except Exception:
            pass


async def _send_translated_comment(temp_path: str, channel_message_id: int) -> None:
    # просто отправляем в админ‑чат через 4 секунды без ответов
    await asyncio.sleep(4)
    try:
        await bot.send_photo(ADMIN_CHAT_ID, FSInputFile(temp_path))
    except Exception as e:
        logger.warning("Failed to send translated meme to admin chat: %s", e)


async def process_meme_translation(channel_message_id: int, content: DraftContent) -> None:
    try:
        if not MEME_TRANSLATE_ENABLED:
            logger.debug("Meme translate disabled.")
            return
        if content.kind not in {"photo", "album", "video"}:
            logger.debug("Unsupported content kind %s", content.kind)
            return
        first = content.items[0] if content.items else None
        if not first or first.get("type") not in {"photo", "document", "video"}:
            logger.debug("First item not photo/document/video; skip.")
            return
        img = None
        if first.get("type") == "video" or content.kind == "video":
            img = await _download_video_frame_image(first.get("file_id"), frame_index=1)
        else:
            raw = await _download_image_bytes(first.get("file_id"))
            if not raw:
                logger.warning("Failed to download image for translation.")
                return
            try:
                img = Image.open(io.BytesIO(raw)).convert("RGB")
            except UnidentifiedImageError:
                logger.warning("Unidentified image; skip.")
                return
        if img is None:
            logger.warning("Could not obtain frame/image for translation.")
            return
        blocks = _ocr_lines(img)
        if not blocks:
            logger.info("No OCR text found; skip translation.")
            return
        share = _english_share(blocks)
        if share < ENGLISH_SHARE_THRESHOLD:
            logger.info("Skip translation: english share %.2f below threshold", share)
            return
        translations = _translate_google([b["text"] for b in blocks]) or [b["text"] for b in blocks]
        try:
            rendered = _render_translation(img, blocks, translations)
        except Exception as e:
            logger.exception("Render failed: %s", e)
            rendered = img
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jpg")
        tmp_path = tmp.name
        tmp.close()
        try:
            rendered.save(tmp_path, format="JPEG")
        except Exception as e:
            logger.warning("Failed to save rendered translation: %s", e)
            with contextlib.suppress(Exception):
                os.remove(tmp_path)
            return
        try:
            await _send_translated_comment(tmp_path, channel_message_id)
            logger.info("Sent translated image for channel msg %s", channel_message_id)
        finally:
            with contextlib.suppress(Exception):
                os.remove(tmp_path)
    except Exception as e:
        logger.exception("process_meme_translation error: %s", e)


def _dhash_hex_from_image(img: Image.Image, *, hash_size: int = 8) -> str:
    resample = Image.Resampling.BILINEAR if hasattr(Image, "Resampling") else Image.BILINEAR
    small = img.resize((hash_size + 1, hash_size), resample=resample).convert("L")
    pixels = np.asarray(small, dtype=np.uint8)
    bits = []
    for row in range(hash_size):
        for col in range(hash_size):
            left = pixels[row, col]
            right = pixels[row, col + 1]
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

def _hash_distance_hex(a: Optional[str], b: Optional[str]) -> Optional[int]:
    if not a or not b:
        return None
    a_raw = str(a).strip()
    b_raw = str(b).strip()
    if not a_raw or not b_raw or len(a_raw) != len(b_raw):
        return None
    return _distance_or_none(_hash_int(a_raw), _hash_int(b_raw))

def _hash_distance_details(
    fp: Dict[str, Any],
    row: aiosqlite.Row,
) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[str], Optional[int]]:
    dist_d = _hash_distance_hex(fp.get("dhash"), row["dhash"])
    dist_p = _hash_distance_hex(fp.get("phash"), row["phash"])
    dist_w = _hash_distance_hex(fp.get("whash"), row["whash"])
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

def _prepare_sift_array(img: Image.Image) -> np.ndarray:
    max_dim = DUPLICATE_SIFT_MAX_DIM
    if max_dim > 0:
        width, height = img.size
        scale = max(width, height) / max_dim if max_dim else 1.0
        if scale > 1.0:
            new_w = max(1, int(round(width / scale)))
            new_h = max(1, int(round(height / scale)))
            resample = Image.Resampling.BILINEAR if hasattr(Image, "Resampling") else Image.BILINEAR
            img = img.resize((new_w, new_h), resample=resample)
    return np.asarray(img, dtype=np.uint8)

def _rootsift(desc: Optional[np.ndarray]) -> Optional[np.ndarray]:
    if desc is None or len(desc) == 0:
        return None
    desc = desc.astype(np.float32, copy=False)
    desc /= desc.sum(axis=1, keepdims=True) + 1e-7
    desc = np.sqrt(desc)
    return desc

def _sift_create(max_features: int) -> Optional[Any]:
    if not DUPLICATE_GEOMETRY_ENABLED or max_features <= 0:
        return None
    if not hasattr(cv2, "SIFT_create"):
        return None
    try:
        return cv2.SIFT_create(
            nfeatures=int(max_features),
            contrastThreshold=float(DUPLICATE_SIFT_CONTRAST_THRESHOLD),
        )
    except Exception:
        try:
            return cv2.SIFT_create(int(max_features))
        except Exception:
            return None

def _sift_features_from_array(
    arr: np.ndarray,
    *,
    max_features: int,
) -> Optional[Tuple[List[Any], np.ndarray]]:
    if arr.size == 0:
        return None
    sift = _sift_create(max_features)
    if sift is None:
        return None
    try:
        kps, desc = sift.detectAndCompute(arr, None)
    except Exception:
        return None
    desc = _rootsift(desc)
    if desc is None or not kps:
        return None
    return list(kps), desc

def _sift_features_from_gray(img: Image.Image) -> Optional[Tuple[List[Any], np.ndarray, Tuple[int, int]]]:
    arr = _prepare_sift_array(img)
    features = _sift_features_from_array(arr, max_features=DUPLICATE_SIFT_NFEATURES)
    if not features:
        return None
    kps, desc = features
    height, width = arr.shape[:2]
    return kps, desc, (width, height)

def _map_keypoints_affine(
    kps: List[Any],
    inv_m: np.ndarray,
    base_size: Tuple[int, int],
) -> Tuple[List[Any], List[int]]:
    width, height = base_size
    mapped: List[Any] = []
    keep_indices: List[int] = []
    for idx, kp in enumerate(kps):
        x, y = kp.pt
        bx = float(inv_m[0, 0] * x + inv_m[0, 1] * y + inv_m[0, 2])
        by = float(inv_m[1, 0] * x + inv_m[1, 1] * y + inv_m[1, 2])
        if bx < 0 or by < 0 or bx >= width or by >= height:
            continue
        try:
            mapped.append(cv2.KeyPoint(bx, by, float(kp.size), float(kp.angle), float(kp.response), int(kp.octave), int(kp.class_id)))
        except Exception:
            mapped.append(kp)
        keep_indices.append(idx)
    return mapped, keep_indices

def _asift_view_array(
    base: np.ndarray,
    *,
    tilt: float,
    degrees: float,
) -> Optional[Tuple[np.ndarray, np.ndarray]]:
    if base.size == 0 or tilt <= 0:
        return None
    height, width = base.shape[:2]
    center = (width * 0.5, height * 0.5)
    rot_m = cv2.getRotationMatrix2D(center, degrees, 1.0)
    rotated = cv2.warpAffine(
        base,
        rot_m,
        (width, height),
        flags=cv2.INTER_LINEAR,
        borderMode=cv2.BORDER_CONSTANT,
        borderValue=128,
    )
    if tilt > 1.01:
        sigma = 0.8 * math.sqrt(max(tilt * tilt - 1.0, 0.0))
        if sigma > 0:
            rotated = cv2.GaussianBlur(rotated, (0, 0), sigmaX=sigma, sigmaY=0.01)
    out_w = max(1, int(round(width / max(tilt, 1e-6))))
    if out_w != width:
        view = cv2.resize(rotated, (out_w, height), interpolation=cv2.INTER_LINEAR)
    else:
        view = rotated
    scale_m = np.array([[1.0 / max(tilt, 1e-6), 0.0, 0.0], [0.0, 1.0, 0.0]], dtype=np.float32)
    rot_3 = np.eye(3, dtype=np.float32)
    rot_3[:2, :] = rot_m.astype(np.float32)
    scale_3 = np.eye(3, dtype=np.float32)
    scale_3[:2, :] = scale_m
    forward = scale_3 @ rot_3
    try:
        inv_m = np.linalg.inv(forward)[:2, :]
    except Exception:
        return None
    return view, inv_m.astype(np.float32)

def _cap_feature_count(
    kps: List[Any],
    desc: np.ndarray,
    max_count: int,
) -> Tuple[List[Any], np.ndarray]:
    if max_count <= 0 or len(kps) <= max_count:
        return kps, desc
    order = np.argsort(np.asarray([float(getattr(kp, "response", 0.0)) for kp in kps], dtype=np.float32))[::-1][:max_count]
    return [kps[int(i)] for i in order], desc[order]

def _serialize_sift_features(
    features: Optional[Tuple[List[Any], np.ndarray, Tuple[int, int]]],
) -> Optional[Tuple[str, bytes, int, int]]:
    if not features:
        return None
    kps, desc, size = features
    if desc is None or len(desc) == 0 or not kps:
        return None
    width, height = size
    keypoints = [
        [
            float(kp.pt[0]),
            float(kp.pt[1]),
            float(kp.size),
            float(kp.angle),
            float(kp.response),
            int(kp.octave),
            int(kp.class_id),
        ]
        for kp in kps
    ]
    buf = io.BytesIO()
    np.save(buf, desc.astype(np.float32, copy=False), allow_pickle=False)
    return json.dumps(keypoints), buf.getvalue(), int(width), int(height)

def _deserialize_sift_features(
    row: Optional[aiosqlite.Row],
) -> Optional[Tuple[List[Any], np.ndarray, Tuple[int, int]]]:
    if not row:
        return None
    try:
        raw_kps = json.loads(row["keypoints_json"] or "[]")
        desc = np.load(io.BytesIO(bytes(row["descriptors"])), allow_pickle=False)
        if desc is None or len(desc) == 0 or not isinstance(raw_kps, list):
            return None
        kps: List[Any] = []
        for raw in raw_kps:
            if not isinstance(raw, list) or len(raw) < 7:
                continue
            kps.append(
                cv2.KeyPoint(
                    float(raw[0]),
                    float(raw[1]),
                    float(raw[2]),
                    float(raw[3]),
                    float(raw[4]),
                    int(raw[5]),
                    int(raw[6]),
                )
            )
        if not kps or len(kps) != len(desc):
            return None
        return kps, desc.astype(np.float32, copy=False), (int(row["width"]), int(row["height"]))
    except Exception:
        return None

async def _save_sift_feature_cache_for_post(
    post_id: int,
    fingerprints: List[Dict[str, Any]],
) -> None:
    if not DUPLICATE_GEOMETRY_ENABLED or not fingerprints:
        return
    for fp in fingerprints:
        features = fp.get("sift_features")
        serialized = _serialize_sift_features(features)
        if not serialized:
            continue
        keypoints_json, descriptors, width, height = serialized
        try:
            await db.upsert_image_feature_cache(
                post_id,
                int(fp["item_index"]),
                "sift",
                DUPLICATE_SIFT_FEATURE_VERSION,
                width,
                height,
                keypoints_json,
                descriptors,
            )
        except Exception as e:
            logger.debug("Failed to cache SIFT features for post %s: %s", post_id, e)

def _asift_features_from_gray(img: Image.Image) -> Optional[Tuple[List[Any], np.ndarray, Tuple[int, int]]]:
    if not DUPLICATE_ASIFT_ENABLED:
        return None
    base = _prepare_sift_array(img)
    if base.size == 0:
        return None
    height, width = base.shape[:2]
    tilts = DUPLICATE_ASIFT_TILTS or [1.0]
    rotations = DUPLICATE_ASIFT_ROTATION_DEGREES or [0.0]
    all_kps: List[Any] = []
    all_desc: List[np.ndarray] = []
    views = 0
    seen: set[Tuple[float, float]] = set()
    for tilt in tilts:
        if tilt <= 0:
            continue
        for degrees in rotations:
            key = (round(float(tilt), 3), round(float(degrees), 3))
            if key in seen:
                continue
            seen.add(key)
            view_info = _asift_view_array(base, tilt=float(tilt), degrees=float(degrees))
            if not view_info:
                continue
            view, inv_m = view_info
            features = _sift_features_from_array(view, max_features=DUPLICATE_SIFT_NFEATURES)
            if not features:
                continue
            kps, desc = features
            mapped, keep_indices = _map_keypoints_affine(kps, inv_m, (width, height))
            if not mapped:
                continue
            if len(keep_indices) != len(desc):
                desc = desc[np.asarray(keep_indices, dtype=np.int32)]
            all_kps.extend(mapped)
            all_desc.append(desc)
            views += 1
            if DUPLICATE_ASIFT_VIEW_LIMIT > 0 and views >= DUPLICATE_ASIFT_VIEW_LIMIT:
                break
        if DUPLICATE_ASIFT_VIEW_LIMIT > 0 and views >= DUPLICATE_ASIFT_VIEW_LIMIT:
            break
    if not all_kps or not all_desc:
        return None
    desc_joined = np.vstack(all_desc).astype(np.float32, copy=False)
    all_kps, desc_joined = _cap_feature_count(all_kps, desc_joined, DUPLICATE_ASIFT_MAX_FEATURES)
    return all_kps, desc_joined, (width, height)

def _point_coverage(points: np.ndarray, size: Tuple[int, int]) -> float:
    if points.shape[0] < 3:
        return 0.0
    width, height = size
    area_total = float(max(1, width * height))
    try:
        hull = cv2.convexHull(points.astype(np.float32).reshape(-1, 1, 2))
        area = float(cv2.contourArea(hull))
    except Exception:
        return 0.0
    return max(0.0, min(1.0, area / area_total))

def _geometry_metrics_from_model(
    src: np.ndarray,
    dst: np.ndarray,
    mask: Optional[np.ndarray],
    transformed: np.ndarray,
    *,
    model: str,
    size_a: Tuple[int, int],
    size_b: Tuple[int, int],
) -> Optional[Dict[str, Any]]:
    if mask is None:
        return None
    inlier_mask = mask.ravel().astype(bool)
    inliers = int(inlier_mask.sum())
    if inliers <= 0 or inliers > len(src):
        return None
    errors = np.linalg.norm(transformed[inlier_mask] - dst[inlier_mask], axis=1)
    if errors.size == 0:
        return None
    rmse = float(math.sqrt(float(np.mean(errors * errors))))
    cov_a = _point_coverage(src[inlier_mask], size_a)
    cov_b = _point_coverage(dst[inlier_mask], size_b)
    good = int(len(src))
    return {
        "model": model,
        "good": good,
        "inliers": inliers,
        "inlier_ratio": inliers / max(1, good),
        "rmse": rmse,
        "coverage_a": cov_a,
        "coverage_b": cov_b,
    }

def _geometry_passes(metrics: Dict[str, Any]) -> bool:
    if int(metrics.get("good") or 0) < DUPLICATE_SIFT_MIN_GOOD:
        return False
    if int(metrics.get("inliers") or 0) < DUPLICATE_SIFT_MIN_INLIERS:
        return False
    if float(metrics.get("inlier_ratio") or 0.0) < DUPLICATE_SIFT_MIN_INLIER_RATIO:
        return False
    if float(metrics.get("rmse") or 9999.0) > DUPLICATE_SIFT_MAX_RMSE:
        return False
    cov_a = float(metrics.get("coverage_a") or 0.0)
    cov_b = float(metrics.get("coverage_b") or 0.0)
    if max(cov_a, cov_b) < DUPLICATE_SIFT_MIN_COVERAGE:
        return False
    if min(cov_a, cov_b) < DUPLICATE_SIFT_MIN_SECONDARY_COVERAGE:
        return False
    return True

def _sift_match_metrics(
    features_a: Optional[Tuple[List[Any], np.ndarray, Tuple[int, int]]],
    features_b: Optional[Tuple[List[Any], np.ndarray, Tuple[int, int]]],
) -> Optional[Dict[str, Any]]:
    if not features_a or not features_b:
        return None
    kps_a, desc_a, size_a = features_a
    kps_b, desc_b, size_b = features_b
    if desc_a is None or desc_b is None or len(desc_a) < 2 or len(desc_b) < 2:
        return None
    try:
        matcher = cv2.BFMatcher(cv2.NORM_L2)
        pairs = matcher.knnMatch(desc_a, desc_b, k=2)
    except Exception:
        return None
    ratio = float(DUPLICATE_SIFT_RATIO)
    good_matches = []
    for pair in pairs:
        if len(pair) < 2:
            continue
        m, n = pair
        if m.distance < ratio * n.distance:
            good_matches.append(m)
    if DUPLICATE_SIFT_MUTUAL and good_matches:
        try:
            reverse_pairs = matcher.knnMatch(desc_b, desc_a, k=2)
            reverse_best: Dict[int, int] = {}
            for pair in reverse_pairs:
                if len(pair) < 2:
                    continue
                m, n = pair
                if m.distance < ratio * n.distance:
                    reverse_best[int(m.queryIdx)] = int(m.trainIdx)
            good_matches = [m for m in good_matches if reverse_best.get(int(m.trainIdx)) == int(m.queryIdx)]
        except Exception:
            pass
    if len(good_matches) < max(4, DUPLICATE_SIFT_MIN_GOOD):
        return None
    src = np.float32([kps_a[m.queryIdx].pt for m in good_matches])
    dst = np.float32([kps_b[m.trainIdx].pt for m in good_matches])
    best: Optional[Dict[str, Any]] = None
    if len(good_matches) >= 4:
        method = getattr(cv2, "USAC_MAGSAC", cv2.RANSAC)
        try:
            h, mask = cv2.findHomography(
                src.reshape(-1, 1, 2),
                dst.reshape(-1, 1, 2),
                method,
                float(DUPLICATE_SIFT_RANSAC_REPROJ),
                None,
                2000,
                0.995,
            )
        except Exception:
            h, mask = None, None
        if h is not None and np.isfinite(h).all():
            try:
                projected = cv2.perspectiveTransform(src.reshape(-1, 1, 2), h).reshape(-1, 2)
                best = _geometry_metrics_from_model(src, dst, mask, projected, model="h", size_a=size_a, size_b=size_b)
            except Exception:
                best = None
    if len(good_matches) >= 3:
        try:
            affine, mask_aff = cv2.estimateAffinePartial2D(
                src,
                dst,
                method=cv2.RANSAC,
                ransacReprojThreshold=float(DUPLICATE_SIFT_RANSAC_REPROJ),
                maxIters=2000,
                confidence=0.995,
                refineIters=10,
            )
        except Exception:
            affine, mask_aff = None, None
        if affine is not None and np.isfinite(affine).all():
            try:
                projected = cv2.transform(src.reshape(-1, 1, 2), affine).reshape(-1, 2)
                aff_metrics = _geometry_metrics_from_model(src, dst, mask_aff, projected, model="a", size_a=size_a, size_b=size_b)
                if aff_metrics is not None:
                    if best is None:
                        best = aff_metrics
                    else:
                        best_key = (int(best["inliers"]), float(best["inlier_ratio"]), -float(best["rmse"]))
                        aff_key = (int(aff_metrics["inliers"]), float(aff_metrics["inlier_ratio"]), -float(aff_metrics["rmse"]))
                        if aff_key > best_key:
                            best = aff_metrics
            except Exception:
                pass
    if best is None or not _geometry_passes(best):
        return None
    return best

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
    for idx, item in enumerate(content.items):
        if not _is_image_item(item):
            continue

        item_type = item.get("type")
        file_id = item.get("file_id") or item.get("hash_file_id")
        if not file_id:
            continue

        raw = await _download_duplicate_image_bytes(str(file_id))
        if not raw:
            continue

        img_pair = _load_image_gray_pair(raw, DUPLICATE_GAUSSIAN_BLUR_RADIUS)
        if not img_pair:
            continue
        img_raw, img_blur, width, height = img_pair
        dhash = _dhash_hex_from_image(img_blur, hash_size=DUPLICATE_IMAGE_HASH_SIZE)
        phash = _phash_hex_from_image(
            img_blur,
            hash_size=DUPLICATE_IMAGE_HASH_SIZE,
            highfreq_size=DUPLICATE_PHASH_HIGHFREQ_SIZE,
        )
        whash = _whash_hex_from_image(
            img_blur,
            hash_size=DUPLICATE_IMAGE_HASH_SIZE,
            image_size=DUPLICATE_WHASH_IMAGE_SIZE,
        )
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
                "image_gray": img_raw,
            }
        )
    return fingerprints

async def compute_video_fingerprints(content: DraftContent) -> List[Dict[str, Any]]:
    fingerprints: List[Dict[str, Any]] = []
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
    thresholds = {
        "d": DUPLICATE_VIDEO_DHASH_THRESHOLD,
        "p": DUPLICATE_VIDEO_PHASH_THRESHOLD,
        "w": DUPLICATE_VIDEO_WHASH_THRESHOLD,
    }
    distances = {"d": dist_d, "p": dist_p, "w": dist_w}
    available = [d for d in distances.values() if d is not None]
    if not available:
        return False, None, None

    hits = 0
    for key, dist in distances.items():
        if dist is None:
            continue
        if dist <= thresholds.get(key, 0):
            hits += 1
    min_dist = min(available)
    if hits < 2 and min_dist > DUPLICATE_VIDEO_SINGLE_HASH_THRESHOLD:
        return False, None, None

    avg_dist = sum(available) / len(available)
    max_dist = max(available)
    if avg_dist > DUPLICATE_VIDEO_FRAME_AVG_DIST_MAX:
        return False, None, None
    if max_dist > DUPLICATE_VIDEO_FRAME_MAX_DIST:
        return False, None, None

    details_parts = []
    if dist_d is not None:
        details_parts.append(f"d={dist_d}")
    if dist_p is not None:
        details_parts.append(f"p={dist_p}")
    if dist_w is not None:
        details_parts.append(f"w={dist_w}")
    details = ",".join(details_parts) if details_parts else None
    return True, details, int(round(avg_dist))

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
) -> Optional[Tuple[int, int, float, int, float, int]]:
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
        score_sum = 0.0
        score_worst = 0
        for q_time, q_frame in q_frames:
            best_idx = None
            best_pick = None
            for idx, (c_time, c_frame) in enumerate(c_frames):
                if idx in used:
                    continue
                diff = abs((q_time + shift) - c_time)
                if diff > time_tol:
                    continue
                ok, _details, frame_score = _video_frame_match(q_frame, c_frame)
                if not ok:
                    continue
                if frame_score is None:
                    continue
                pick_key = (frame_score, diff)
                if best_pick is None or pick_key < best_pick:
                    best_pick = pick_key
                    best_idx = idx
            if best_idx is not None:
                used.add(best_idx)
                matched += 1
                best_frame_score = int(best_pick[0]) if best_pick is not None else 0
                score_sum += best_frame_score
                if best_frame_score > score_worst:
                    score_worst = best_frame_score
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
        if matched <= 0:
            continue
        avg_score = score_sum / float(matched)
        if avg_score > DUPLICATE_VIDEO_FRAME_AVG_DIST_MAX:
            continue
        if score_worst > DUPLICATE_VIDEO_FRAME_MAX_DIST:
            continue
        key = (matched, len(bins), -avg_score, -abs(shift))
        if best_key is None or key > best_key:
            best_key = key
            best = (matched, total, float(shift), len(bins), float(avg_score), int(score_worst))
    return best

async def _collect_matches_from_candidates(
    fp: Dict[str, Any],
    candidates: List[aiosqlite.Row],
    thresholds: Dict[str, int],
    match_type: str,
) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []
    for row in candidates:
        dist_d = _hash_distance_hex(fp.get("dhash"), row["dhash"])
        dist_p = _hash_distance_hex(fp.get("phash"), row["phash"])
        dist_w = _hash_distance_hex(fp.get("whash"), row["whash"])
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
        fp_matches: List[Dict[str, Any]] = []
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
            matched, total, shift, bins, avg_score, worst_score = match_info
            ratio = matched / max(1, total)
            ratio_penalty = max(0, int(round((1.0 - ratio) * 70.0)))
            hash_penalty = max(0, int(round(avg_score * 1.5)))
            distance = ratio_penalty + hash_penalty
            details = f"v={matched}/{total},h={avg_score:.1f}"
            if worst_score > int(round(avg_score)):
                details = f"{details},hm={worst_score}"
            if abs(shift) > 0.001:
                details = f"{details},s={shift:.2f}"
            fp_matches.append(
                {
                    "item_index": fp["item_index"],
                    "match_type": "video_deep",
                    "post_id": row["post_id"],
                    "distance": distance,
                    "details": details,
                }
            )
        fp_matches.sort(key=lambda item: (int(item.get("distance", 0)), int(item.get("post_id", 0))))
        if DUPLICATE_VIDEO_MAX_MATCHES_PER_ITEM > 0:
            fp_matches = fp_matches[:DUPLICATE_VIDEO_MAX_MATCHES_PER_ITEM]
        matches.extend(fp_matches)
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
        file_id = item.get("file_id") or item.get("hash_file_id")
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

async def _get_sift_features_for_post(
    post_id: int,
    cache: Dict[int, List[Tuple[List[Any], np.ndarray, Tuple[int, int]]]],
    *,
    asift: bool = False,
) -> List[Tuple[List[Any], np.ndarray, Tuple[int, int]]]:
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
    feats: List[Tuple[List[Any], np.ndarray, Tuple[int, int]]] = []
    for idx, item in enumerate(content.items):
        if not _is_image_item(item):
            continue
        item_index = int(item.get("item_index") if item.get("item_index") is not None else idx)
        algo = "asift" if asift else "sift"
        cached_row = await db.get_image_feature_cache(
            post_id,
            item_index,
            algo,
            DUPLICATE_SIFT_FEATURE_VERSION,
        )
        cached_features = _deserialize_sift_features(cached_row)
        if cached_features:
            feats.append(cached_features)
            continue
        file_id = item.get("file_id") or item.get("hash_file_id")
        if not file_id:
            continue
        raw = await _download_duplicate_image_bytes(str(file_id))
        if not raw:
            continue
        img_info = _load_image_gray(raw, 0.0)
        if not img_info:
            continue
        img_gray, _w, _h = img_info
        features = _asift_features_from_gray(img_gray) if asift else _sift_features_from_gray(img_gray)
        if features:
            feats.append(features)
            serialized = _serialize_sift_features(features)
            if serialized:
                keypoints_json, descriptors, width, height = serialized
                try:
                    await db.upsert_image_feature_cache(
                        post_id,
                        item_index,
                        algo,
                        DUPLICATE_SIFT_FEATURE_VERSION,
                        width,
                        height,
                        keypoints_json,
                        descriptors,
                    )
                except Exception as e:
                    logger.debug("Failed to cache %s features for post %s: %s", algo, post_id, e)
    cache[post_id] = feats
    return feats

def _rank_geometry_candidates(
    fp: Dict[str, Any],
    candidates_pool: List[aiosqlite.Row],
) -> List[Tuple[int, int, Optional[str]]]:
    scored_hash: List[Tuple[int, int]] = []
    scored_size: List[Tuple[float, int]] = []
    details_by_post: Dict[int, Optional[str]] = {}
    score_by_post: Dict[int, int] = {}
    for row in candidates_pool:
        _d, _p, _w, details, score = _hash_distance_details(fp, row)
        post_id = int(row["post_id"])
        if score is not None:
            prev = score_by_post.get(post_id)
            if prev is None or score < prev:
                details_by_post[post_id] = details
                score_by_post[post_id] = score
            scored_hash.append((score, post_id))
        size_score = _size_similarity_score(fp, row)
        if size_score is not None:
            scored_size.append((size_score, post_id))
    scored_hash.sort(key=lambda item: (item[0], item[1]))
    scored_size.sort(key=lambda item: (item[0], item[1]))
    selected: List[Tuple[int, int, Optional[str]]] = []
    seen: set[int] = set()
    for score, post_id in scored_hash[: max(0, DUPLICATE_SIFT_TOPK)]:
        if post_id in seen:
            continue
        seen.add(post_id)
        selected.append((score, post_id, details_by_post.get(post_id)))
    for size_score, post_id in scored_size[: max(0, DUPLICATE_SIFT_TOPK_SIZE)]:
        if post_id in seen:
            continue
        seen.add(post_id)
        score = score_by_post.get(post_id)
        if score is None:
            score = 1000 + int(round(size_score * 100.0))
        selected.append((score, post_id, details_by_post.get(post_id)))
    return selected

def _geometry_detail_part(kind: str, metrics: Dict[str, Any]) -> str:
    label = "asift" if kind == "asift_geometry" else "sift"
    return (
        f"{label}={int(metrics['good'])},"
        f"inl={int(metrics['inliers'])},"
        f"ir={float(metrics['inlier_ratio']):.2f},"
        f"err={float(metrics['rmse']):.1f},"
        f"cov={float(metrics['coverage_a']):.2f}/{float(metrics['coverage_b']):.2f},"
        f"m={metrics.get('model') or '?'}"
    )

def _geometry_distance(metrics: Dict[str, Any], hash_score: int) -> int:
    rmse_part = int(round(float(metrics.get("rmse") or 0.0) * 10.0))
    ratio_part = int(round((1.0 - float(metrics.get("inlier_ratio") or 0.0)) * 50.0))
    coverage_part = int(round((1.0 - max(float(metrics.get("coverage_a") or 0.0), float(metrics.get("coverage_b") or 0.0))) * 20.0))
    hash_part = min(max(hash_score, 0), 64) // 8
    return max(0, rmse_part + ratio_part + coverage_part + hash_part)

def _best_sift_metrics(
    query_features: Optional[Tuple[List[Any], np.ndarray, Tuple[int, int]]],
    candidate_features: List[Tuple[List[Any], np.ndarray, Tuple[int, int]]],
) -> Optional[Dict[str, Any]]:
    if not query_features or not candidate_features:
        return None
    best: Optional[Dict[str, Any]] = None
    for cand_features in candidate_features:
        metrics = _sift_match_metrics(query_features, cand_features)
        if metrics is None:
            continue
        if best is None:
            best = metrics
            continue
        best_key = (
            int(best["inliers"]),
            float(best["inlier_ratio"]),
            max(float(best["coverage_a"]), float(best["coverage_b"])),
            -float(best["rmse"]),
        )
        metrics_key = (
            int(metrics["inliers"]),
            float(metrics["inlier_ratio"]),
            max(float(metrics["coverage_a"]), float(metrics["coverage_b"])),
            -float(metrics["rmse"]),
        )
        if metrics_key > best_key:
            best = metrics
    return best

async def _store_feature_cache(
    post_id: int,
    item_index: int,
    algo: str,
    features: Optional[Tuple[List[Any], np.ndarray, Tuple[int, int]]],
) -> bool:
    serialized = _serialize_sift_features(features)
    if not serialized:
        return False
    keypoints_json, descriptors, width, height = serialized
    await db.upsert_image_feature_cache(
        post_id,
        item_index,
        algo,
        DUPLICATE_SIFT_FEATURE_VERSION,
        width,
        height,
        keypoints_json,
        descriptors,
    )
    return True

async def _cache_features_for_content(
    post_id: int,
    content: DraftContent,
    *,
    include_asift: bool = False,
    force: bool = False,
) -> Tuple[int, int, int]:
    cached = 0
    skipped = 0
    errors = 0
    for idx, item in enumerate(content.items):
        if not _is_image_item(item):
            skipped += 1
            continue
        item_index = int(item.get("item_index") if item.get("item_index") is not None else idx)
        existing_sift = None
        existing_asift = None
        if not force:
            existing_sift = await db.get_image_feature_cache(
                post_id,
                item_index,
                "sift",
                DUPLICATE_SIFT_FEATURE_VERSION,
            )
            if include_asift:
                existing_asift = await db.get_image_feature_cache(
                    post_id,
                    item_index,
                    "asift",
                    DUPLICATE_SIFT_FEATURE_VERSION,
                )
            if existing_sift and (not include_asift or existing_asift):
                skipped += 1
                continue
        file_id = item.get("file_id") or item.get("hash_file_id")
        if not file_id:
            skipped += 1
            continue
        raw = await _download_duplicate_image_bytes(str(file_id))
        if not raw:
            errors += 1
            continue
        img_info = _load_image_gray(raw, 0.0)
        if not img_info:
            errors += 1
            continue
        img_gray, _w, _h = img_info
        try:
            if not existing_sift and await _store_feature_cache(post_id, item_index, "sift", _sift_features_from_gray(img_gray)):
                cached += 1
            elif not existing_sift:
                skipped += 1
            if include_asift and not existing_asift:
                if await _store_feature_cache(post_id, item_index, "asift", _asift_features_from_gray(img_gray)):
                    cached += 1
        except Exception as e:
            errors += 1
            logger.debug("Feature cache failed for post %s item %s: %s", post_id, item_index, e)
    return cached, skipped, errors

async def annotate_matches_with_geometry(
    fingerprints: List[Dict[str, Any]],
    matches: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not DUPLICATE_GEOMETRY_ENABLED or DUPLICATE_SIFT_TOPK <= 0:
        return matches
    if not fingerprints:
        return matches
    candidates_pool: List[aiosqlite.Row] = []
    if DUPLICATE_FULLSCAN_LIMIT > 0:
        candidates_pool = await db.list_published_fingerprints(DUPLICATE_FULLSCAN_LIMIT)
    if not candidates_pool:
        return matches
    deadline = (
        time.monotonic() + DUPLICATE_GEOMETRY_TIMEOUT_SECONDS
        if DUPLICATE_GEOMETRY_TIMEOUT_SECONDS > 0
        else None
    )
    fp_by_idx = {fp["item_index"]: fp for fp in fingerprints}
    exact_items = {
        int(m.get("item_index", -1))
        for m in matches
        if m.get("match_type") == "unique_id"
    }
    sift_cache: Dict[int, List[Tuple[List[Any], np.ndarray, Tuple[int, int]]]] = {}
    asift_cache: Dict[int, List[Tuple[List[Any], np.ndarray, Tuple[int, int]]]] = {}
    for item_idx, fp in fp_by_idx.items():
        if deadline is not None and time.monotonic() >= deadline:
            break
        if int(item_idx) in exact_items:
            continue
        selected = _rank_geometry_candidates(fp, candidates_pool)
        if not selected:
            continue
        query_sift = fp.get("sift_features")
        if query_sift is None:
            img_gray = fp.get("image_gray")
            if img_gray is not None:
                query_sift = _sift_features_from_gray(img_gray)
                if query_sift is not None:
                    fp["sift_features"] = query_sift
        if query_sift is None:
            continue
        query_asift: Optional[Tuple[List[Any], np.ndarray, Tuple[int, int]]] = None
        verified_count = 0
        for rank, (hash_score, post_id, hash_details) in enumerate(selected):
            if deadline is not None and time.monotonic() >= deadline:
                break
            best_kind = "sift_geometry"
            best_metrics: Optional[Dict[str, Any]] = None
            cand_sifts = await _get_sift_features_for_post(post_id, sift_cache, asift=False)
            best_metrics = _best_sift_metrics(query_sift, cand_sifts)
            if (
                best_metrics is None
                and DUPLICATE_ASIFT_ENABLED
                and rank < max(0, DUPLICATE_ASIFT_TOPK)
                and hash_score <= DUPLICATE_ASIFT_MAX_HASH_SCORE
            ):
                if query_asift is None:
                    img_gray = fp.get("image_gray")
                    if img_gray is not None:
                        query_asift = _asift_features_from_gray(img_gray)
                cand_asifts = await _get_sift_features_for_post(post_id, asift_cache, asift=True)
                asift_metrics = _best_sift_metrics(query_asift, cand_asifts)
                if asift_metrics is not None:
                    best_kind = "asift_geometry"
                    best_metrics = asift_metrics
            if best_metrics is None:
                continue
            verified_count += 1
            geom_part = _geometry_detail_part(best_kind, best_metrics)
            details = f"{hash_details},{geom_part}" if hash_details else geom_part
            distance = _geometry_distance(best_metrics, hash_score)
            updated = False
            for m in matches:
                if int(m.get("item_index", -1)) != item_idx:
                    continue
                if int(m.get("post_id", -1)) != post_id:
                    continue
                if m.get("match_type") == "unique_id":
                    updated = True
                    break
                m["match_type"] = best_kind
                m["distance"] = distance
                existing = m.get("details")
                if existing and geom_part not in existing:
                    m["details"] = f"{existing},{geom_part}"
                else:
                    m["details"] = details
                updated = True
                break
            if updated:
                if (
                    DUPLICATE_GEOMETRY_MAX_MATCHES_PER_ITEM > 0
                    and verified_count >= DUPLICATE_GEOMETRY_MAX_MATCHES_PER_ITEM
                ):
                    break
                continue
            matches.append(
                {
                    "item_index": item_idx,
                    "match_type": best_kind,
                    "post_id": post_id,
                    "distance": distance,
                    "details": details,
                }
            )
            if (
                DUPLICATE_GEOMETRY_MAX_MATCHES_PER_ITEM > 0
                and verified_count >= DUPLICATE_GEOMETRY_MAX_MATCHES_PER_ITEM
            ):
                break
    return matches

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
            matches = await annotate_matches_with_geometry(fingerprints, matches)
        except Exception as e:
            logger.warning("Image geometry verification failed: %s", e)
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
        if match_type in {"sift_geometry", "asift_geometry"}:
            filtered.append(match)
            continue
        details = match.get("details") or ""
        if "sift=" in details or "asift=" in details:
            filtered.append(match)
            continue
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

def _duplicate_post_ids_from_matches(matches: Optional[List[Dict[str, Any]]]) -> List[int]:
    if not matches:
        return []
    seen: set[int] = set()
    ordered: List[Tuple[int, int]] = []
    for match in matches:
        try:
            post_id = int(match["post_id"])
            distance = int(match.get("distance", 0))
        except Exception:
            continue
        if post_id in seen:
            continue
        seen.add(post_id)
        ordered.append((distance, post_id))
    ordered.sort(key=lambda item: (item[0], item[1]))
    return [post_id for _distance, post_id in ordered]

def _duplicate_post_ids_from_info(duplicate_info: Optional[str]) -> List[int]:
    if not duplicate_info:
        return []
    text = str(duplicate_info)
    if "возможный повтор" not in text.lower():
        return []
    result: List[int] = []
    seen: set[int] = set()
    for match in re.finditer(r"#id(\d+)", text):
        post_id = int(match.group(1))
        if post_id in seen:
            continue
        seen.add(post_id)
        result.append(post_id)
    return result

def _format_duplicate_command_lines(
    post_ids: List[int],
    *,
    command: str,
    label: str,
) -> str:
    if not post_ids:
        return ""
    return f"{label}:\n/{command} {post_ids[0]}"

def _build_duplicate_command_block(post_ids: List[int], *, admin_view: bool = False) -> str:
    if not post_ids:
        return ""
    lines = [
        _format_duplicate_command_lines(
            post_ids,
            command="post",
            label=PUBLIC_DUPLICATE_NOTICE_LABEL,
        )
    ]
    if admin_view:
        lines.append(
            _format_duplicate_command_lines(
                post_ids,
                command="catpost",
                label=ADMIN_DUPLICATE_NOTICE_LABEL,
            )
        )
    return "\n".join(line for line in lines if line)

def _build_duplicate_command_block_from_info(duplicate_info: Optional[str], *, admin_view: bool = False) -> str:
    return _build_duplicate_command_block(
        _duplicate_post_ids_from_info(duplicate_info),
        admin_view=admin_view,
    )

def _build_admin_duplicate_block(matches: Optional[List[Dict[str, Any]]], *, admin_view: bool = False) -> str:
    if not matches:
        return ""
    duplicate_info = format_duplicate_info(matches, always_show=False)
    if not duplicate_info:
        return ""
    command_block = _build_duplicate_command_block(
        _duplicate_post_ids_from_matches(matches),
        admin_view=admin_view,
    )
    if command_block:
        return f"{duplicate_info}\n{command_block}"
    return duplicate_info

def _submit_confirm_base_text(
    duplicate_block: Optional[str],
    *,
    scan_failed: bool = False,
    media_checked: bool = True,
) -> str:
    if duplicate_block:
        return ADMIN_SUBMIT_CONFIRM_DUPLICATE_TEXT
    if scan_failed:
        return ADMIN_SUBMIT_CONFIRM_SCAN_FAILED_TEXT
    if media_checked:
        return ADMIN_SUBMIT_CONFIRM_TEXT
    return ADMIN_SUBMIT_CONFIRM_NO_SCAN_TEXT

def _submit_scan_progress_bar(elapsed: float) -> Tuple[str, int]:
    expected = max(3.0, SUBMIT_DUPLICATE_SCAN_EXPECTED_SECONDS)
    progress = min(0.94, max(0.05, elapsed / expected))
    width = 18
    filled = int(round(width * progress))
    filled = max(1, min(width - 1, filled))
    bar = "[" + "#" * filled + "." * (width - filled) + "]"
    return bar, int(progress * 100)

def _format_submit_duplicate_scan_progress(started_at: float, fast_matches: List[Dict[str, Any]]) -> str:
    elapsed = max(0.0, time.monotonic() - started_at)
    bar, percent = _submit_scan_progress_bar(elapsed)
    if elapsed < 2:
        step = "проверяю точные совпадения"
    elif elapsed < 6:
        step = "сравниваю с опубликованными постами"
    else:
        step = "перепроверяю самые похожие варианты"
    fast_line = ""
    if fast_matches:
        fast_line = f"\nУже вижу возможное совпадение: {len(fast_matches)}."
    return (
        "Проверка повторок выполняется.\n\n"
        "Пожалуйста, подождите: ищу похожие опубликованные посты. "
        "Если найду повторку, лучше отменить отправку.\n\n"
        f"{bar} {percent}%\n"
        f"{step.capitalize()}.\n"
        f"Обычно это занимает несколько секунд.{fast_line}"
    )

async def _animate_submit_duplicate_scan(
    message: Message,
    deep_task: asyncio.Task,
    fast_matches: List[Dict[str, Any]],
) -> None:
    started_at = time.monotonic()
    last_text = ""
    while not deep_task.done():
        text = _format_submit_duplicate_scan_progress(started_at, fast_matches)
        if text != last_text:
            try:
                await bot.edit_message_text(text, chat_id=message.chat.id, message_id=message.message_id)
                last_text = text
            except Exception as e:
                if "message is not modified" not in str(e):
                    logger.debug("Failed to update submit duplicate scan progress: %s", e)
        await asyncio.sleep(max(0.2, SUBMIT_DUPLICATE_SCAN_UPDATE_SECONDS))

def _compose_admin_submit_message(base_text: str, duplicate_block: Optional[str]) -> str:
    main_text = base_text.strip()
    if duplicate_block:
        main_text = main_text.replace(
            "\n\nВажно: если такой же пост уже публиковался, заявка будет отклонена.",
            "",
        ).strip()
    parts = [main_text]
    if duplicate_block:
        parts.append(duplicate_block.strip())
    text = "\n\n".join(part for part in parts if part)
    if len(text) <= 4096:
        return text
    return text[:4095] + "…"

async def _refresh_admin_duplicate_message(token: str) -> None:
    session = admin_duplicate_sessions.get(token)
    if not session:
        return
    message_id = session.get("message_id")
    chat_id = session.get("chat_id")
    if not message_id or not chat_id:
        return
    text = _compose_admin_submit_message(session.get("base_text") or "", session.get("duplicate_block"))
    if text == session.get("rendered_text") and session.get("rendered_has_markup") == bool(session.get("reply_markup")):
        return
    lock = session.get("lock")
    if lock is None:
        return
    async with lock:
        fresh = admin_duplicate_sessions.get(token)
        if fresh is not session:
            return
        text = _compose_admin_submit_message(session.get("base_text") or "", session.get("duplicate_block"))
        if text == session.get("rendered_text") and session.get("rendered_has_markup") == bool(session.get("reply_markup")):
            return
        try:
            await bot.edit_message_text(
                text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=session.get("reply_markup"),
            )
            session["rendered_text"] = text
            session["rendered_has_markup"] = bool(session.get("reply_markup"))
        except Exception as e:
            if "message is not modified" in str(e):
                session["rendered_text"] = text
                session["rendered_has_markup"] = bool(session.get("reply_markup"))
            else:
                logger.warning("Failed to refresh admin duplicate message for token %s: %s", token, e)

async def _persist_duplicate_result_for_post(
    post_id: int,
    image_fps: List[Dict[str, Any]],
    video_fps: List[Dict[str, Any]],
    matches: List[Dict[str, Any]],
    *,
    fallback_matches: Optional[List[Dict[str, Any]]] = None,
) -> None:
    merged_matches: List[Dict[str, Any]] = []
    if fallback_matches:
        merged_matches.extend(fallback_matches)
    merged_matches.extend(matches)
    if image_fps or video_fps:
        try:
            if image_fps:
                await db.add_image_fingerprints(post_id, image_fps)
            if video_fps:
                await db.add_video_fingerprints(post_id, video_fps)
        except Exception as e:
            logger.warning("Failed to save fingerprints for post %s: %s", post_id, e)
        dup_info = format_duplicate_info(merged_matches, always_show=True)
    elif merged_matches:
        dup_info = format_duplicate_info(merged_matches, always_show=True)
    else:
        dup_info = "Повторки: не удалось проверить"
    try:
        await db.set_post_duplicate_info(post_id, dup_info)
    except Exception as e:
        logger.warning("Failed to store duplicate info for post %s: %s", post_id, e)

async def _watch_admin_duplicate_session(token: str) -> None:
    session = admin_duplicate_sessions.get(token)
    if not session:
        return
    deep_task = session.get("deep_task")
    if deep_task is None:
        return
    try:
        image_fps, video_fps, deep_matches = await deep_task
        failed = False
    except asyncio.CancelledError:
        return
    except Exception as e:
        logger.warning("Deep duplicate check failed for admin session %s: %s", token, e)
        image_fps, video_fps, deep_matches = [], [], []
        failed = True

    session = admin_duplicate_sessions.get(token)
    if not session:
        return
    session["deep_done"] = True
    session["deep_result"] = (image_fps, video_fps, deep_matches)
    session["deep_failed"] = failed

    merged_matches = list(session.get("fast_matches") or [])
    merged_matches.extend(deep_matches)
    duplicate_block = _build_admin_duplicate_block(merged_matches, admin_view=bool(session.get("admin_view")))
    if duplicate_block != (session.get("duplicate_block") or ""):
        session["duplicate_block"] = duplicate_block
        await _refresh_admin_duplicate_message(token)

    post_id = session.get("post_id")
    if post_id and not session.get("saved_to_db"):
        if failed and not merged_matches:
            with contextlib.suppress(Exception):
                await db.set_post_duplicate_info(post_id, "Повторки: ошибка проверки")
        else:
            await _persist_duplicate_result_for_post(
                post_id,
                image_fps,
                video_fps,
                deep_matches,
                fallback_matches=session.get("fast_matches"),
            )
        session["saved_to_db"] = True

    if session.get("confirmed") and session.get("post_id") and session.get("saved_to_db"):
        admin_duplicate_sessions.pop(token, None)

async def _cancel_admin_duplicate_session(token: Optional[str]) -> None:
    if not token:
        return
    session = admin_duplicate_sessions.pop(token, None)
    if not session:
        return
    watcher_task = session.get("watcher_task")
    if watcher_task and not watcher_task.done():
        watcher_task.cancel()
    deep_task = session.get("deep_task")
    if deep_task and not deep_task.done():
        deep_task.cancel()

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
    apply_watermark: bool = False,
) -> Tuple[int, List[int]]:
    """Send stored content to a chat, returning first message id and list of all ids."""
    message_ids: List[int] = []
    use_watermark = apply_watermark and WATERMARK_ENABLED
    if content.kind == "album":
        builder = MediaGroupBuilder()
        separate_caption_message = bool(reply_markup or force_buttons_message)
        first = True
        for item in content.items:
            item_caption = caption if first and caption and not separate_caption_message else None
            first = False
            if use_watermark and _is_image_item(item):
                wm_input = await _watermark_input_for_item(item)
                if wm_input:
                    if item["type"] == "photo":
                        builder.add_photo(media=wm_input, caption=item_caption)
                        continue
                    if item["type"] == "document":
                        builder.add_document(media=wm_input, caption=item_caption)
                        continue
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
        wm_input = None
        if use_watermark:
            wm_input = await _watermark_input_for_item(content.items[0])
        if wm_input:
            msg = await bot.send_photo(dest_chat, wm_input, caption=caption, reply_markup=reply_markup)
        else:
            msg = await bot.send_photo(dest_chat, content.items[0]["file_id"], caption=caption, reply_markup=reply_markup)
    elif content.kind == "video":
        msg = await bot.send_video(dest_chat, content.items[0]["file_id"], caption=caption, reply_markup=reply_markup)
    elif content.kind == "animation":
        msg = await bot.send_animation(dest_chat, content.items[0]["file_id"], caption=caption, reply_markup=reply_markup)
    elif content.kind == "document":
        wm_input = None
        if use_watermark and _is_image_item(content.items[0]):
            wm_input = await _watermark_input_for_item(content.items[0])
        if wm_input:
            msg = await bot.send_document(dest_chat, wm_input, caption=caption, reply_markup=reply_markup)
        else:
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

async def send_post_only(dest_chat: int, content: DraftContent) -> Tuple[int, List[int]]:
    message_ids: List[int] = []
    caption_text = escape(content.caption or "")

    if content.kind == "album":
        if not content.items:
            raise ValueError("Album has no stored items")
        builder = MediaGroupBuilder()
        first = True
        for item in content.items:
            item_caption = caption_text if first and caption_text else None
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
        return messages[0].message_id, message_ids

    if content.kind == "photo":
        if not content.items:
            raise ValueError("Photo has no stored items")
        msg = await bot.send_photo(dest_chat, content.items[0]["file_id"], caption=caption_text or None)
    elif content.kind == "video":
        if not content.items:
            raise ValueError("Video has no stored items")
        msg = await bot.send_video(dest_chat, content.items[0]["file_id"], caption=caption_text or None)
    elif content.kind == "animation":
        if not content.items:
            raise ValueError("Animation has no stored items")
        msg = await bot.send_animation(dest_chat, content.items[0]["file_id"], caption=caption_text or None)
    elif content.kind == "document":
        if not content.items:
            raise ValueError("Document has no stored items")
        msg = await bot.send_document(dest_chat, content.items[0]["file_id"], caption=caption_text or None)
    elif content.kind == "audio":
        if not content.items:
            raise ValueError("Audio has no stored items")
        msg = await bot.send_audio(dest_chat, content.items[0]["file_id"], caption=caption_text or None)
    elif content.kind == "voice":
        if not content.items:
            raise ValueError("Voice has no stored items")
        msg = await bot.send_voice(dest_chat, content.items[0]["file_id"])
    elif content.kind == "video_note":
        if not content.items:
            raise ValueError("Video note has no stored items")
        msg = await bot.send_video_note(dest_chat, content.items[0]["file_id"])
    elif content.kind == "text":
        msg = await bot.send_message(dest_chat, caption_text)
    else:
        raise ValueError("Unsupported content kind")

    message_ids.append(msg.message_id)
    return msg.message_id, message_ids

def _debug_short_value(value: Any, *, limit: int = 180) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    text = text.replace("\r", "\\r").replace("\n", "\\n")
    if len(text) <= limit:
        return text
    head = max(20, limit // 2)
    tail = max(12, limit - head - 3)
    return f"{text[:head]}...{text[-tail:]}"

def _debug_short_token(value: Any, *, head: int = 12, tail: int = 8) -> str:
    if value is None:
        return "NULL"
    text = str(value)
    if len(text) <= head + tail + 3:
        return text
    return f"{text[:head]}...{text[-tail:]} len={len(text)}"

def _debug_kv(label: str, value: Any, *, limit: int = 180) -> str:
    return f"{label}: {escape(_debug_short_value(value, limit=limit))}"

def _debug_has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip()) and value.strip().lower() not in {"null", "none"}
    if isinstance(value, (list, tuple, dict, set)):
        return bool(value)
    return True

def _debug_format_dt(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return escape(_debug_short_value(text, limit=60))
    if dt.tzinfo is not None:
        dt = dt.astimezone(TZ)
    return dt.strftime("%m.%d.%y %H:%M")

def _debug_format_bytes(value: Any) -> str:
    try:
        size = int(value)
    except Exception:
        return escape(str(value))
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size / (1024 * 1024):.1f} MB"

def _debug_format_duration_ms(value: Any) -> str:
    try:
        ms = int(value)
    except Exception:
        return escape(str(value))
    if ms < 1000:
        return f"{ms} ms"
    return f"{ms / 1000:.1f}s"

def _debug_translate_status(value: Any) -> str:
    text = str(value or "")
    return {
        "pending": "на модерации",
        "scheduled": "в отложке",
        "published": "опубликован",
        "rejected": "отклонён",
    }.get(text, text)

def _debug_translate_media(value: Any) -> str:
    text = str(value or "")
    return {
        "photo": "фото",
        "album": "альбом",
        "video": "видео",
        "animation": "гифка",
        "document": "документ",
        "audio": "аудио",
        "voice": "голосовое",
        "video_note": "видеокружок",
        "text": "текст",
    }.get(text, text)

def _debug_user_label(user: Optional[aiosqlite.Row]) -> str:
    if not user:
        return "не найден"
    pieces: List[str] = []
    username = user["username"] if "username" in user.keys() else None
    tg_id = user["tg_id"] if "tg_id" in user.keys() else None
    hashtag = user["hashtag"] if "hashtag" in user.keys() else None
    if username:
        pieces.append(f"@{escape(str(username))}")
    if tg_id:
        pieces.append(f"tg {int(tg_id)}")
    if "id" in user.keys() and user["id"]:
        pieces.append(f"user_id {int(user['id'])}")
    if hashtag:
        pieces.append(f"#{escape(str(hashtag))}")
    return ", ".join(pieces) if pieces else "не найден"

def _debug_split_text(text: str, *, limit: int = 3500) -> List[str]:
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    current: List[str] = []
    current_len = 0
    for line in text.splitlines():
        add_len = len(line) + (1 if current else 0)
        if current and current_len + add_len > limit:
            chunks.append("\n".join(current))
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += add_len
    if current:
        chunks.append("\n".join(current))
    return chunks

def _feature_keypoint_count(row: aiosqlite.Row) -> int:
    try:
        raw = json.loads(row["keypoints_json"] or "[]")
        return len(raw) if isinstance(raw, list) else 0
    except Exception:
        return 0

async def _format_duplicate_debug_lines(duplicate_info: Optional[str]) -> List[str]:
    if not _debug_has_value(duplicate_info):
        return []
    text = str(duplicate_info).replace("\\n", "\n")
    rows: List[str] = []
    for match in re.finditer(r"#id(\d+)\s*(\([^)\n]*\))?", text):
        post_id = int(match.group(1))
        metrics = (match.group(2) or "").strip()
        label = f"#id{post_id}"
        dup_post = await db.get_post(post_id)
        if dup_post:
            dup_user = await db.get_user_by_id(dup_post["user_id"])
            if dup_user and dup_user["hashtag"]:
                label += f" #{escape(str(dup_user['hashtag']))}"
        if metrics:
            label += f" {escape(metrics)}"
        rows.append(label)
    if rows:
        return ["<b>Повторки</b>", *rows]
    compact = re.sub(r"^\s*Повторки:\s*", "", text, flags=re.IGNORECASE).strip()
    return ["<b>Повторки</b>", escape(_debug_short_value(compact, limit=700))]

def _format_content_debug(content: Optional[DraftContent]) -> List[str]:
    lines: List[str] = ["<b>Контент</b>"]
    if content is None:
        lines.append("media_json не парсится")
        return lines
    lines.append(f"Вид: {escape(_debug_translate_media(content.kind))}")
    if content.items:
        lines.append(f"Медиа: {len(content.items)}")
    for idx, item in enumerate(content.items):
        details: List[str] = [escape(_debug_translate_media(item.get("type", content.kind)))]
        width = item.get("width")
        height = item.get("height")
        if _debug_has_value(width) and _debug_has_value(height):
            details.append(f"{int(width)}x{int(height)}")
        if _debug_has_value(item.get("file_size")):
            details.append(_debug_format_bytes(item.get("file_size")))
        if _debug_has_value(item.get("duration")):
            details.append(f"длительность={escape(str(item.get('duration')))}s")
        for key in (
            "mime_type",
            "file_name",
        ):
            if key in item and _debug_has_value(item.get(key)):
                label = {"mime_type": "mime", "file_name": "файл"}[key]
                details.append(f"{label}={escape(_debug_short_value(item.get(key), limit=80))}")
        lines.append(f"{idx + 1}. " + ", ".join(details))
    return lines

async def _format_post_debug_info(
    post: aiosqlite.Row,
    user: Optional[aiosqlite.Row],
    content: Optional[DraftContent],
    image_fps: List[aiosqlite.Row],
    video_fps: List[aiosqlite.Row],
    feature_rows: List[aiosqlite.Row],
    votes: List[aiosqlite.Row],
    likes: int,
    dislikes: int,
) -> str:
    lines: List[str] = []
    lines.append(f"<b>Пост #id{post['id']}</b>")
    if _debug_has_value(post["status"]):
        lines.append(f"Статус: {escape(_debug_translate_status(post['status']))}")
    if _debug_has_value(post["media_type"]):
        lines.append(f"Тип: {escape(_debug_translate_media(post['media_type']))}")
    lines.append(f"Автор: {_debug_user_label(user)}")
    if user and user["banned"]:
        lines.append("Бан: да")

    date_fields = (
        ("Создан", "created_at"),
        ("Одобрен", "approved_at"),
        ("Запланирован", "scheduled_at"),
        ("Опубликован", "published_at"),
    )
    for label, key in date_fields:
        if key in post.keys() and _debug_has_value(post[key]):
            lines.append(f"{label}: {_debug_format_dt(post[key])}")
    if _debug_has_value(post["channel_message_id"]):
        lines.append(f"Канал: https://t.me/ikarus251/{int(post['channel_message_id'])}")
    if _debug_has_value(post["admin_chat_id"]) or _debug_has_value(post["admin_message_id"]) or _debug_has_value(post["admin_message_ids"]):
        admin_parts: List[str] = []
        if _debug_has_value(post["admin_chat_id"]):
            admin_parts.append(f"чат={post['admin_chat_id']}")
        if _debug_has_value(post["admin_message_id"]):
            admin_parts.append(f"сообщение={post['admin_message_id']}")
        if _debug_has_value(post["admin_message_ids"]):
            admin_parts.append(f"сообщения={escape(_debug_short_value(post['admin_message_ids'], limit=160))}")
        lines.append("Админ-чат: " + ", ".join(admin_parts))
    if _debug_has_value(post["notified_status"]) and post["notified_status"] != post["status"]:
        lines.append(f"Уведомление: {escape(_debug_translate_status(post['notified_status']))}")
    if _debug_has_value(post["reason"]):
        lines.append(f"Причина: {escape(_debug_short_value(post['reason'], limit=500))}")
    if _debug_has_value(post["caption"]):
        lines.append(f"Подпись: {escape(_debug_short_value(post['caption'], limit=500))}")

    duplicate_lines = await _format_duplicate_debug_lines(post["duplicate_info"] if "duplicate_info" in post.keys() else None)
    if duplicate_lines:
        lines.append("")
        lines.extend(duplicate_lines)

    if likes or dislikes or votes:
        lines.append("")
        lines.append(f"<b>Голоса</b>")
        lines.append(f"Лайки: {likes}, дизлайки: {dislikes}")
    if votes:
        vote_values = {"like": "лайк", "dislike": "дизлайк"}
        lines.append(", ".join(f"{int(v['admin_id'])}: {vote_values.get(str(v['value']), escape(str(v['value'])))}" for v in votes[:30]))
        if len(votes) > 30:
            lines.append(f"ещё {len(votes) - 30}")

    lines.append("")
    lines.extend(_format_content_debug(content))

    if image_fps:
        lines.append("")
        lines.append(f"<b>Хеши изображений: {len(image_fps)}</b>")
        for idx, row in enumerate(image_fps, start=1):
            details: List[str] = [f"item={row['item_index']}", escape(_debug_translate_media(row["kind"]))]
            if _debug_has_value(row["width"]) and _debug_has_value(row["height"]):
                details.append(f"{int(row['width'])}x{int(row['height'])}")
            if _debug_has_value(row["file_size"]):
                details.append(_debug_format_bytes(row["file_size"]))
            if _debug_has_value(row["file_unique_id"]):
                details.append(f"unique={escape(_debug_short_token(row['file_unique_id']))}")
            lines.append(f"{idx}. " + ", ".join(details))
            hash_parts = []
            for key in ("dhash", "phash", "whash"):
                if key in row.keys() and _debug_has_value(row[key]):
                    hash_parts.append(f"{key}={escape(str(row[key]))}")
            if hash_parts:
                lines.append(", ".join(hash_parts))

    if video_fps:
        lines.append("")
        lines.append(f"<b>Хеши видео: {len(video_fps)}</b>")
        for idx, row in enumerate(video_fps, start=1):
            frames = _parse_video_frames(row["frame_hashes"])
            first_frame = frames[0] if frames else {}
            details: List[str] = [f"item={row['item_index']}", escape(_debug_translate_media(row["kind"]))]
            if _debug_has_value(row["width"]) and _debug_has_value(row["height"]):
                details.append(f"{int(row['width'])}x{int(row['height'])}")
            if _debug_has_value(row["file_size"]):
                details.append(_debug_format_bytes(row["file_size"]))
            if _debug_has_value(row["duration_ms"]):
                details.append(_debug_format_duration_ms(row["duration_ms"]))
            if _debug_has_value(row["fps"]):
                details.append(f"fps={row['fps']}")
            details.append(f"кадров={len(frames)}")
            lines.append(f"{idx}. " + ", ".join(details))
            if first_frame:
                parts = []
                for src, dst in (("t", "t"), ("d", "dhash"), ("p", "phash"), ("w", "whash")):
                    if _debug_has_value(first_frame.get(src)):
                        parts.append(f"{dst}={escape(str(first_frame.get(src)))}")
                if parts:
                    lines.append("первый кадр: " + ", ".join(parts))

    if feature_rows:
        lines.append("")
        lines.append(f"<b>Признаки изображений: {len(feature_rows)}</b>")
        for idx, row in enumerate(feature_rows, start=1):
            details = [
                f"item={row['item_index']}",
                f"{escape(str(row['algo']))} v{row['version']}",
            ]
            if _debug_has_value(row["width"]) and _debug_has_value(row["height"]):
                details.append(f"{int(row['width'])}x{int(row['height'])}")
            details.append(f"точек={_feature_keypoint_count(row)}")
            if _debug_has_value(row["descriptor_bytes"]):
                details.append(f"desc={_debug_format_bytes(row['descriptor_bytes'])}")
            if _debug_has_value(row["created_at"]):
                details.append(_debug_format_dt(row["created_at"]))
            lines.append(f"{idx}. " + ", ".join(details))

    lines.append("")
    lines.append("<b>Настройки поиска</b>")
    lines.append(
        f"hash={DUPLICATE_IMAGE_HASH_SIZE}, blur={DUPLICATE_GAUSSIAN_BLUR_RADIUS}, "
        f"SIFT top={DUPLICATE_SIFT_TOPK}+{DUPLICATE_SIFT_TOPK_SIZE}, "
        f"dim={DUPLICATE_SIFT_MAX_DIM}, features={DUPLICATE_SIFT_NFEATURES}, "
        f"ASIFT top={DUPLICATE_ASIFT_TOPK}, timeout={DUPLICATE_GEOMETRY_TIMEOUT_SECONDS}s"
    )
    return "\n".join(lines)

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
    n_softmax = min(6, cap)


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
    final_caption = build_publish_caption(caption_base, hashtag, is_media=content.kind != "text")
    channel_message_id = None
    try:
        message_id, _ = await send_content_copy(
            CHANNEL_ID,
            content,
            caption=final_caption,
            apply_watermark=True,
        )
        channel_message_id = message_id
        await db.set_post_status(
            post_row["id"],
            "published",
            channel_message_id=channel_message_id,
            notified_status="published",
            published_at=datetime.now(TZ),
        )
        if channel_message_id and content.kind in {"photo", "album", "video"}:
            asyncio.create_task(process_meme_translation(channel_message_id, content))
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
    menu = await user_menu_for(message.from_user.id)
    await message.answer(
        "Добро пожаловать в предложку канала Недолёт Мыслей Икара. "
        "Бот управляется с помощью кнопок, которые находятся ниже. "
        "Советуем также прочитать раздел 'Помощь'",
        reply_markup=menu,
    )

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

    rows = await db.list_recent_posts(limit)
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

        has_images = content_has_images(content)
        has_videos = content_has_videos(content)
        if not has_images and not has_videos:
            no_media += 1
            continue
        need_image_fps = has_images
        need_video_fps = has_videos
        if not force:
            if has_images and await db.has_image_fingerprints(row["id"]):
                need_image_fps = False
            if has_videos and await db.has_video_fingerprints(row["id"]):
                need_video_fps = False
            if not need_image_fps and not need_video_fps:
                skipped += 1
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
            saved_any = False
            if image_fps and (force or need_image_fps):
                await db.add_image_fingerprints(row["id"], image_fps)
                saved_any = True
            if video_fps and (force or need_video_fps):
                await db.add_video_fingerprints(row["id"], video_fps)
                saved_any = True
            if not saved_any and not force:
                skipped += 1
                continue
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

@dp.message(Command(commands=["backfillfeatures"]))
async def backfill_features(message: Message, command: CommandObject):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    parts = (command.args or "").split()
    if not parts:
        await message.answer("Формат: /backfillfeatures <кол-во постов> [force] [asift]")
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
    include_asift = any(p.lower() in {"asift", "affine", "full"} for p in parts[1:])
    limit = min(requested, DUPLICATE_BACKFILL_MAX_POSTS)
    if limit != requested:
        await message.answer(f"Ограничиваю бэкфилл до {limit} постов.")

    if force:
        rows = await db.list_recent_posts(limit)
    else:
        algo = "asift" if include_asift else "sift"
        rows = await db.list_recent_image_posts_without_feature_cache(
            limit,
            algo,
            DUPLICATE_SIFT_FEATURE_VERSION,
        )
    if not rows:
        await message.answer("Нет постов без feature-cache для бэкфилла.")
        return

    total = len(rows)
    rows = list(reversed(rows))
    mode_parts = ["форс" if force else "обычный", "asift" if include_asift else "sift"]
    mode_label = ", ".join(mode_parts)
    status_msg = await message.answer(f"Бэкфилл признаков ({mode_label}): найдено {total} постов, начинаю...")

    processed = 0
    cached = 0
    skipped = 0
    no_media = 0
    errors = 0
    last_update = time.monotonic()

    for idx, row in enumerate(rows, start=1):
        content = _draft_content_from_media_json(row["media_json"] or "")
        if not content:
            skipped += 1
            continue
        if not content_has_images(content):
            no_media += 1
            continue
        try:
            if force:
                await db.delete_image_feature_cache(row["id"])
            cached_one, skipped_one, errors_one = await _cache_features_for_content(
                row["id"],
                content,
                include_asift=include_asift,
                force=force,
            )
            cached += cached_one
            skipped += skipped_one
            errors += errors_one
            processed += 1
        except Exception as e:
            errors += 1
            logger.warning("Feature backfill failed for post %s: %s", row["id"], e)

        if time.monotonic() - last_update >= 2:
            await status_msg.edit_text(
                f"Бэкфилл признаков ({mode_label}): {idx}/{total}. "
                f"Постов {processed}, признаков {cached}, без медиа {no_media}, пропущено {skipped}, ошибки {errors}."
            )
            last_update = time.monotonic()

    await status_msg.edit_text(
        f"Бэкфилл признаков завершён ({mode_label}): всего {total}, постов {processed}, "
        f"признаков {cached}, без медиа {no_media}, пропущено {skipped}, ошибки {errors}."
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

@dp.message(Command(commands=["catpost"]))
async def cat_post(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        return
    arg = (command.args or "").strip()
    if not arg:
        await message.answer("Формат: /catpost 999")
        return
    lowered = arg.lower()
    if lowered.startswith("#id") or lowered.startswith("id"):
        await message.answer("Нужно указать только номер поста, без #id и id. Пример: /catpost 999")
        return
    if not arg.isdigit():
        await message.answer("Номер поста должен быть числом. Пример: /catpost 999")
        return
    post_id = int(arg)
    post = await db.get_post(post_id)
    if not post:
        await message.answer("Пост не найден.")
        return

    content = _draft_content_from_media_json(post["media_json"] or "")
    user = await db.get_user_by_id(post["user_id"])
    likes, dislikes = await db.get_vote_counts(post_id)
    votes = await db.list_votes_for_post(post_id)
    image_fps = await db.list_image_fingerprints_for_post(post_id)
    video_fps = await db.list_video_fingerprints_for_post(post_id)
    feature_rows = await db.list_image_feature_cache_for_post(post_id)

    debug_info = await _format_post_debug_info(
        post,
        user,
        content,
        image_fps,
        video_fps,
        feature_rows,
        votes,
        likes,
        dislikes,
    )

    is_single_photo = (
        content is not None
        and content.kind == "photo"
        and len(content.items) == 1
        and content.items[0].get("type") == "photo"
        and bool(content.items[0].get("file_id"))
    )
    if is_single_photo:
        chunks = _debug_split_text(debug_info, limit=950)
        try:
            await bot.send_photo(
                message.chat.id,
                content.items[0]["file_id"],
                caption=chunks[0] if chunks else None,
            )
            for chunk in chunks[1:]:
                await message.answer(chunk)
            return
        except Exception as e:
            logger.warning("Failed to send /catpost %s photo with debug caption: %s", post_id, e)

    sent_content = False
    if content and (content.kind == "text" or content.items):
        try:
            await send_post_only(message.chat.id, content)
            sent_content = True
        except Exception as e:
            logger.warning("Failed to send /catpost %s from stored content: %s", post_id, e)

    if not sent_content:
        if content is None:
            await message.answer("У поста не сохранился контент для надёжного просмотра. Ниже только данные из БД.")
        elif content.kind != "text" and not content.items:
            await message.answer(
                "Для этого поста в базе не сохранились медиа. Ниже только данные из БД."
            )
        else:
            await message.answer("Не удалось отправить сохранённое содержимое. Ниже данные из БД.")

    for chunk in _debug_split_text(debug_info):
        await message.answer(chunk)

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

# ручная модерация суперадмина 

def _build_review_keyboard(post_id: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    buttons = []
    nav_row = []
    if has_prev:
        nav_row.append(InlineKeyboardButton(text="⬅️", callback_data=f"review:prev:{post_id}"))
    nav_row.append(InlineKeyboardButton(text="✅ Одобрить", callback_data=f"review:approve:{post_id}"))
    nav_row.append(InlineKeyboardButton(text="❌ Отклонить", callback_data=f"review:reject:{post_id}"))
    if has_next:
        nav_row.append(InlineKeyboardButton(text="➡️", callback_data=f"review:next:{post_id}"))
    buttons.append(nav_row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def _load_review_queue(user_id: int):
    rows = await db.list_pending_posts(limit=200)
    review_sessions[user_id] = {"posts": [r["id"] for r in rows], "idx": 0, "msg_ids": []}

async def _cleanup_review_messages(user_id: int):
    session = review_sessions.get(user_id)
    if not session:
        return
    msg_ids = session.get("msg_ids") or []
    for mid in msg_ids:
        try:
            await bot.delete_message(chat_id=user_id, message_id=mid)
        except Exception:
            continue
    session["msg_ids"] = []

async def _show_review_post(user_id: int) -> bool:
    session = review_sessions.get(user_id)
    if not session or not session.get("posts"):
        await bot.send_message(user_id, "Нет постов на модерации.")
        return False
    idx = session.get("idx", 0)
    posts = session["posts"]
    if idx < 0:
        idx = 0
    if idx >= len(posts):
        idx = len(posts) - 1
    session["idx"] = idx
    post_id = posts[idx]

    # убрать старые сообщения
    await _cleanup_review_messages(user_id)

    post = await db.get_post(post_id)
    if not post or post["status"] != "pending":
        # убрать из очереди и показать следующий
        posts.pop(idx)
        if not posts:
            await bot.send_message(user_id, "Нет постов на модерации.")
            return False
        session["idx"] = min(idx, len(posts) - 1)
        return await _show_review_post(user_id)

    user_row = await db.get_user_by_id(post["user_id"])
    hashtag = user_row["hashtag"] if user_row else ""
    author = f"@{user_row['username']}" if user_row and user_row["username"] else str(user_row["tg_id"]) if user_row else ""
    likes, dislikes = await db.get_vote_counts(post_id)
    caption = format_review_caption_ru(post, hashtag=hashtag, author=author, likes=likes, dislikes=dislikes)
    content = _draft_content_from_media_json(post["media_json"])
    if not content:
        await bot.send_message(user_id, f"Пост #id{post_id} без контента, пропускаю.")
        posts.pop(idx)
        if not posts:
            await bot.send_message(user_id, "Нет постов на модерации.")
            return False
        session["idx"] = min(idx, len(posts) - 1)
        return await _show_review_post(user_id)

    has_prev = idx > 0
    has_next = idx < len(posts) - 1
    markup = _build_review_keyboard(post_id, has_prev, has_next)
    try:
        msg_id, msg_ids = await send_content_copy(
            user_id,
            content,
            caption=caption,
            reply_markup=markup,
            force_buttons_message=True,
        )
        session["msg_ids"] = msg_ids
    except Exception as e:
        logger.error("Failed to send review post %s to %s: %s", post_id, user_id, e)
        await bot.send_message(user_id, f"Не удалось отправить пост #id{post_id}, пропускаю...")
        posts.pop(idx)
        if not posts:
            await bot.send_message(user_id, "Нет постов на модерации.")
            return False
        session["idx"] = min(idx, len(posts) - 1)
        return await _show_review_post(user_id)
    return True

async def _advance_review(user_id: int, delta: int):
    session = review_sessions.get(user_id)
    if not session or not session.get("posts"):
        await bot.send_message(user_id, "Нет постов на модерации.")
        return
    session["idx"] = max(0, min(session["idx"] + delta, len(session["posts"]) - 1))
    await _show_review_post(user_id)

@dp.message(F.text == "📝 Модерация")
@dp.message(Command(commands=["review"]))
async def superadmin_review(message: Message):
    if not await is_super_admin(message.from_user.id):
        return
    if message.chat.type != "private":
        await message.answer("Команда доступна только в личных сообщениях.")
        return
    await _load_review_queue(message.from_user.id)
    if not review_sessions.get(message.from_user.id, {}).get("posts"):
        await message.answer("Нет постов на модерации.", reply_markup=SUPER_MENU)
        return
    ok = await _show_review_post(message.from_user.id)
    if ok:
        await message.answer("Режим модерации.", reply_markup=SUPER_MENU)

@dp.callback_query(F.data.startswith("review:"))
async def handle_review_actions(callback: CallbackQuery):
    if not await is_super_admin(callback.from_user.id):
        return await callback.answer("Недостаточно прав.", show_alert=True)
    try:
        _prefix, action, pid = callback.data.split(":", 2)
        post_id = int(pid)
    except Exception:
        return await callback.answer()

    session = review_sessions.get(callback.from_user.id)
    if not session or post_id not in (session.get("posts") or []):
        await callback.answer("Сессия устарела, перезапускаю.")
        await _load_review_queue(callback.from_user.id)
        await _show_review_post(callback.from_user.id)
        return

    if action == "prev":
        await callback.answer()
        await _advance_review(callback.from_user.id, -1)
        return
    if action == "next":
        await callback.answer()
        await _advance_review(callback.from_user.id, 1)
        return
    if action == "approve":
        await callback.answer("Одобряю...")
        try:
            scheduled_dt = await schedule_post(post_id)
        except Exception as e:
            logger.error("Approve failed for %s (schedule): %s", post_id, e)
            await callback.message.answer("Не удалось одобрить пост.")
            return
        # уведомления не должны ломать поток
        try:
            await notify_user_status(post_id, "scheduled", None, scheduled_dt.isoformat())
        except Exception as e:
            logger.warning("Notify scheduled failed for %s: %s", post_id, e)
        try:
            await update_admin_view(post_id)
        except Exception as e:
            logger.warning("Update admin view failed for %s: %s", post_id, e)
    elif action == "reject":
        await callback.answer("Отклоняю...")
        try:
            await db.set_post_status(post_id, "rejected")
        except Exception as e:
            logger.error("Reject failed for %s (db): %s", post_id, e)
            await callback.message.answer("Не удалось отклонить пост.")
            return
        try:
            await notify_user_status(post_id, "rejected", None, None)
        except Exception as e:
            logger.warning("Notify reject failed for %s: %s", post_id, e)
        try:
            await update_admin_view(post_id)
        except Exception as e:
            logger.warning("Update admin view failed for %s: %s", post_id, e)
    else:
        await callback.answer()
        return

    # убрать текущий пост из очереди и показать следующий
    posts = session.get("posts") or []
    idx = session.get("idx", 0)
    if post_id in posts:
        pos = posts.index(post_id)
        posts.pop(pos)
        if pos <= idx and idx > 0:
            idx -= 1
    session["idx"] = idx
    session["posts"] = posts
    await _cleanup_review_messages(callback.from_user.id)
    if not posts:
        await bot.send_message(callback.from_user.id, "Больше нет постов на модерации.")
        return
    await _show_review_post(callback.from_user.id)

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
        "/catpost id — показать содержимое и данные БД поста.\n"
        "/pausebot, /resumebot — пауза/возобновление работы бота.\n"
        "/superadd id, /superdel id, /superlist — управлять суперадминами.\n"
        "/cancelpost id - снять пост из отложки и отменить.\n"
        "/broadcast текст или ответом - рассылка всем пользователям.\n"
        "/ban_hashtag tag, /unban_hashtag tag - бан/разбан по хэштегу.\n"
        "/backfilldups N [force] - бэкфилл отпечатков и дублей для последних N постов.\n"
        "/backfillfeatures N [force] [asift] - прогреть SIFT/ASIFT feature-cache.\n"
        "/backfillchannel N [force] - импорт постов из канала (если нет в БД) + отпечатки.",
    )

@dp.message(Command(commands=["top"]))
@dp.message(F.text == "🏆 Топ")
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
    await message.answer(text, reply_markup=await user_menu_for(message.from_user.id))

@dp.message(Command(commands=["myposts"]))
@dp.message(F.text == "\U0001f5c2 Мои посты")
async def my_posts(message: Message):
    if message.chat.type != "private":
        return
    user = await db.get_user_by_tg(message.from_user.id)
    if not user:
        await message.answer("Нажмите /start для начала.", reply_markup=await user_menu_for(message.from_user.id))
        return
    rows = await db.list_posts_by_user(user["id"], limit=20)
    if not rows:
        await message.answer("Вы ещё не отправляли посты.", reply_markup=await user_menu_for(message.from_user.id))
        return
    # показать от старых к новым
    rows = list(reversed(rows))

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
        label = _status_with_icon(status)
        likes, dislikes = await db.get_vote_counts(r["id"])
        ts_raw = r["published_at"] or r["scheduled_at"] or r["approved_at"] or r["created_at"]
        ts_text = "время неизвестно"
        if ts_raw:
            try:
                dt = datetime.fromisoformat(ts_raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=TZ)
                ts_text = format_short_date(dt)
            except Exception:
                ts_text = ts_raw
        time_prefix = "от"
        lines.append(
            f"• #id{r['id']} — {label}\n"
            f"  {time_prefix} {ts_text}     👍{likes}/👎{dislikes}"
        )
    text = "Ваши посты:\n" + "\n".join(lines)
    await message.answer(text, reply_markup=await user_menu_for(message.from_user.id))

@dp.message(F.text == "ℹ️ О боте")
async def about_bot(message: Message):
    if message.chat.type != "private":
        return
    await message.answer("Бот написан с нуля. Вдохновлённый ботом @SilverCumBot", reply_markup=await user_menu_for(message.from_user.id))

@dp.message(Command(commands=["vps"]))
@dp.message(F.text.in_(["🌐 ВПС", "ВПС"]))
async def show_vps_menu(message: Message):
    if message.chat.type != "private":
        return
    user = await db.get_user_by_tg(message.from_user.id)
    if not user:
        await message.answer("Нажмите /start для начала.", reply_markup=await user_menu_for(message.from_user.id))
        return
    text = await build_vps_menu_text(message.from_user.id)
    await message.answer(
        text,
        reply_markup=build_vps_profiles_keyboard(),
        disable_web_page_preview=True,
    )

@dp.callback_query(F.data == "vps:menu")
async def cb_vps_menu(callback: CallbackQuery):
    if callback.message is None:
        return await callback.answer()
    if callback.message.chat.type != "private":
        return await callback.answer()
    await callback.answer()
    text = await build_vps_menu_text(callback.from_user.id)
    try:
        await callback.message.edit_text(
            text,
            reply_markup=build_vps_profiles_keyboard(),
            disable_web_page_preview=True,
        )
    except Exception:
        await callback.message.answer(
            text,
            reply_markup=build_vps_profiles_keyboard(),
            disable_web_page_preview=True,
        )

@dp.callback_query(F.data.startswith("vps:profile:"))
async def cb_vps_profile(callback: CallbackQuery):
    if callback.message is None:
        return await callback.answer()
    if callback.message.chat.type != "private":
        return await callback.answer()
    profile_key = callback.data.split(":", 2)[-1]
    if profile_key not in VPS_PROFILES:
        return await callback.answer("Неизвестный профиль.", show_alert=True)
    await callback.answer("Подбираю строку подключения...")
    await send_vps_config_to_user(callback.message, callback.from_user.id, profile_key)

@dp.callback_query(F.data.startswith("vps:more:"))
async def cb_vps_more(callback: CallbackQuery):
    if callback.message is None:
        return await callback.answer()
    if callback.message.chat.type != "private":
        return await callback.answer()
    profile_key = callback.data.split(":", 2)[-1]
    if profile_key not in VPS_PROFILES:
        return await callback.answer("Неизвестный профиль.", show_alert=True)
    await callback.answer("Подбираю новый вариант...")
    await send_vps_config_to_user(callback.message, callback.from_user.id, profile_key)

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
    await callback.message.answer("Главное меню:", reply_markup=await user_menu_for(callback.from_user.id))
@dp.message(HashtagFlow.waiting_hashtag)
async def receive_hashtag(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return
    raw = (message.text or "").strip().lstrip("#")
    if raw.lower() == "cancel" or message.text == "/cancel":
        await state.clear()
        await message.answer("Отменено.", reply_markup=await user_menu_for(message.from_user.id))
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
        await callback.message.answer("Главное меню:", reply_markup=await user_menu_for(callback.from_user.id))
        return
    if await db.is_hashtag_taken(new_tag, exclude_tg_id=callback.from_user.id):
        await callback.message.edit_text("Такой хэштег уже занят. Попробуйте другой.")
        await state.set_state(HashtagFlow.waiting_hashtag)
        return
    await db.set_hashtag(callback.from_user.id, new_tag)
    await state.clear()
    await callback.message.edit_text("✅ Спасибо, что придумали персональный хэштег", reply_markup=None)
    await callback.message.answer("Главное меню:", reply_markup=await user_menu_for(callback.from_user.id))

@dp.message(Command(commands=["post"]))
async def public_post(message: Message, command: CommandObject):
    if message.chat.type != "private":
        return
    arg = (command.args or "").strip()
    if not arg:
        await message.answer("Формат: /post 999")
        return
    lowered = arg.lower()
    if lowered.startswith("#id"):
        arg = arg[3:].strip()
    elif lowered.startswith("id"):
        arg = arg[2:].strip()
    if not arg.isdigit():
        await message.answer("Номер поста должен быть числом. Пример: /post 999")
        return
    post_id = int(arg)
    post = await db.get_post(post_id)
    if not post or post["status"] != "published":
        await message.answer("Опубликованный пост не найден.")
        return
    content = _draft_content_from_media_json(post["media_json"] or "")
    if not content or (content.kind != "text" and not content.items):
        await message.answer("У этого поста не сохранился контент для просмотра.")
        return
    wait_text = await _check_public_post_view_limit(message.from_user.id)
    if wait_text:
        await message.answer(f"Лимит просмотра постов исчерпан. Попробуйте через {wait_text}.")
        return
    try:
        await send_post_only(message.chat.id, content)
    except Exception as e:
        logger.warning("Failed to send /post %s from stored content: %s", post_id, e)
        await message.answer("Не удалось показать этот пост.")

@dp.message(Command(commands=["propose", "offerpost", "predlozhka"]))
@dp.message(F.text.in_(["📨 Предложить пост", "📮 Предложить пост", "Предложить пост"]))
async def propose_post(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return
    user = await db.get_user_by_tg(message.from_user.id)
    if not user:
        await message.answer("Нажмите /start для начала.")
        return
    if not user["hashtag"]:
        await message.answer("Сначала задайте хэштег.", reply_markup=await user_menu_for(message.from_user.id))
        return
    if user["banned"]:
        await message.answer("Вы забанены. Для вопросов - @p0st_shit")
        return
    is_admin_user = await is_admin(message.from_user.id)
    if not is_admin_user:
        window_start = datetime.utcnow() - timedelta(minutes=RATE_LIMIT_WINDOW_MINUTES)
        recent = await db.count_posts_since(user["id"], window_start.isoformat())
        if recent >= RATE_LIMIT_MAX_POSTS:
            await message.answer(
                f"Лимит: не больше {RATE_LIMIT_MAX_POSTS} постов за {RATE_LIMIT_WINDOW_MINUTES} минут. Попробуйте позже.",
                reply_markup=await user_menu_for(message.from_user.id),
            )
            return
        pending = await db.get_pending_count(user["id"])
        if pending >= MAX_PENDING_PER_USER:
            await message.answer("Слишком много заявок. Подождите пока модерация разберётся.")
            return
    await state.set_state(SubmissionFlow.waiting_content)
    await state.update_data(is_admin=is_admin_user)
    await message.answer(
        "Отправьте один пост (текст, фото, видео или альбом). После отправки я уточню подтверждение.",
        reply_markup=SUBMIT_CANCEL_KB,
    )

async def _remove_reply_keyboard_silently(chat_id: int) -> None:
    try:
        service_message = await bot.send_message(
            chat_id,
            ".",
            reply_markup=ReplyKeyboardRemove(),
            disable_notification=True,
        )
    except Exception as e:
        logger.debug("Failed to send reply-keyboard remover: %s", e)
        return
    with contextlib.suppress(Exception):
        await bot.delete_message(chat_id=chat_id, message_id=service_message.message_id)

async def _send_submit_control_message(message: Message, text: str) -> Message:
    previous_message_id = last_submit_control_message.get(message.chat.id)
    if previous_message_id:
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id=message.chat.id, message_id=previous_message_id)
    await _remove_reply_keyboard_silently(message.chat.id)
    control_message = await message.answer(text)
    last_submit_control_message[message.chat.id] = control_message.message_id
    return control_message

async def _clear_submit_control_message(chat_id: int) -> None:
    message_id = last_submit_control_message.pop(chat_id, None)
    if not message_id:
        return
    with contextlib.suppress(Exception):
        await bot.delete_message(chat_id=chat_id, message_id=message_id)

async def _edit_submit_control_to_confirm(
    control_message: Message,
    text: str,
    reply_markup: InlineKeyboardMarkup,
) -> Message:
    last_submit_control_message.pop(control_message.chat.id, None)
    try:
        await bot.edit_message_text(
            text,
            chat_id=control_message.chat.id,
            message_id=control_message.message_id,
            reply_markup=reply_markup,
        )
        return control_message
    except Exception as e:
        logger.debug("Failed to edit submit control message into confirm: %s", e)
        with contextlib.suppress(Exception):
            await bot.delete_message(chat_id=control_message.chat.id, message_id=control_message.message_id)
        return await bot.send_message(control_message.chat.id, text, reply_markup=reply_markup)

def _public_post_wait_text(seconds: int) -> str:
    seconds = max(1, int(seconds))
    minutes = seconds // 60
    rest = seconds % 60
    if minutes and rest:
        return f"{minutes} мин {rest} сек"
    if minutes:
        return f"{minutes} мин"
    return f"{rest} сек"

async def _check_public_post_view_limit(user_id: int) -> Optional[str]:
    if await is_admin(user_id):
        return None
    now = time.monotonic()
    record = public_post_view_limits.get(user_id)
    if not record:
        public_post_view_limits[user_id] = {
            "count": 1,
            "window_start": now,
            "blocked_until": 0.0,
        }
        return None
    blocked_until = float(record.get("blocked_until") or 0.0)
    if blocked_until > now:
        return _public_post_wait_text(math.ceil(blocked_until - now))
    window_start = float(record.get("window_start") or now)
    if now - window_start >= PUBLIC_POST_VIEW_TIMEOUT_SECONDS:
        record["count"] = 1
        record["window_start"] = now
        record["blocked_until"] = 0.0
        return None
    count = int(record.get("count") or 0)
    if count >= PUBLIC_POST_VIEW_LIMIT:
        record["blocked_until"] = now + PUBLIC_POST_VIEW_TIMEOUT_SECONDS
        record["count"] = 0
        record["window_start"] = now
        return _public_post_wait_text(PUBLIC_POST_VIEW_TIMEOUT_SECONDS)
    record["count"] = count + 1
    return None

def _parse_report_post_id(text: Optional[str]) -> Optional[int]:
    raw = (text or "").strip()
    match = re.fullmatch(r"(?:#?\s*id\s*)?(\d{1,7})", raw, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        post_id = int(match.group(1))
    except Exception:
        return None
    if post_id <= 0 or post_id > 1_000_000:
        return None
    return post_id

def _report_user_label(tg_user: Any) -> str:
    user_id = int(getattr(tg_user, "id", 0) or 0)
    username = getattr(tg_user, "username", None)
    full_name = getattr(tg_user, "full_name", None) or getattr(tg_user, "first_name", None) or ""
    if username:
        return f"@{escape(username)} (<a href=\"tg://user?id={user_id}\">{user_id}</a>)"
    if full_name:
        return f"<a href=\"tg://user?id={user_id}\">{escape(full_name)}</a> ({user_id})"
    return f"<a href=\"tg://user?id={user_id}\">{user_id}</a>"

def _report_db_user_label(user: Optional[aiosqlite.Row]) -> str:
    if not user:
        return "не найден"
    tg_id = int(user["tg_id"]) if user["tg_id"] is not None else 0
    username = user["username"] if "username" in user.keys() else None
    if username:
        return f"@{escape(str(username))} ({tg_id})"
    return str(tg_id)

def _report_short_text(value: Any, *, limit: int = 500) -> str:
    text = str(value or "").strip()
    text = text.replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."

def _extract_duplicate_post_ids(duplicate_info: Optional[str], *, exclude_post_id: Optional[int] = None) -> List[int]:
    found: List[int] = []
    seen: set[int] = set()
    for match in re.finditer(r"#id(\d+)", duplicate_info or ""):
        try:
            post_id = int(match.group(1))
        except Exception:
            continue
        if exclude_post_id is not None and post_id == int(exclude_post_id):
            continue
        if post_id in seen:
            continue
        seen.add(post_id)
        found.append(post_id)
    return found

async def _format_report_post_line(post_id: int, *, label: str) -> str:
    post = await db.get_post(post_id)
    if not post:
        return f"{label}: #id{post_id} (не найден)"
    user = await db.get_user_by_id(post["user_id"])
    hashtag = user["hashtag"] if user and user["hashtag"] else ""
    parts = [f"{label}: #id{post['id']}"]
    parts.append(f"отправитель={_report_db_user_label(user)}")
    if hashtag:
        parts.append(f"хэштег=#{escape(str(hashtag))}")
    parts.append(f"админ: /catpost {post['id']}")
    return "\n".join(parts)

async def _send_report_to_superadmins(text: str) -> int:
    sent = 0
    chunks = _debug_split_text(text, limit=3500)
    for admin_id in sorted(await get_super_admin_ids()):
        try:
            for chunk in chunks:
                await bot.send_message(admin_id, chunk, disable_web_page_preview=True)
            sent += 1
        except Exception as e:
            logger.warning("Failed to send report to superadmin %s: %s", admin_id, e)
    return sent

async def _build_mnemosyne_report_text(
    tg_user: Any,
    post: aiosqlite.Row,
    comment: Optional[str],
) -> str:
    duplicate_ids = _extract_duplicate_post_ids(post["duplicate_info"], exclude_post_id=int(post["id"]))
    lines = [
        "<b>Жалоба на Мнемосину: поиск повторок</b>",
        f"Отправитель: {_report_user_label(tg_user)}",
        "",
        await _format_report_post_line(int(post["id"]), label="Пост пользователя"),
    ]
    if duplicate_ids:
        lines.append("")
        lines.append("Посты, которые бот счёл повторками:")
        for dup_id in duplicate_ids[:10]:
            lines.append(await _format_report_post_line(dup_id, label="Кандидат"))
    else:
        lines.append("")
        lines.append("Посты-кандидаты из duplicate_info не извлечены.")
    if comment:
        lines.append("")
        lines.append("Комментарий пользователя:")
        lines.append(f"\"{escape(_report_short_text(comment, limit=1200))}\"")
    return "\n".join(lines)

def _build_text_report_text(tg_user: Any, report_kind: str, text: str) -> str:
    title = {
        "chronos": "Жалоба/предложение по Хроносу: планировщик отложки",
        "other": "Жалоба на другое, связанное с ботом",
    }.get(report_kind, "Жалоба по боту")
    return "\n".join(
        [
            f"<b>{escape(title)}</b>",
            f"Отправитель: {_report_user_label(tg_user)}",
            "",
            "Текст пользователя:",
            f"\"{escape(_report_short_text(text, limit=2500))}\"",
        ]
    )

async def _finish_report_for_user(
    chat_id: int,
    tg_user: Any,
    state: FSMContext,
    report_text: str,
    *,
    edit_message: Optional[Message] = None,
) -> None:
    sent = await _send_report_to_superadmins(report_text)
    await state.clear()
    user_id = int(getattr(tg_user, "id", 0) or 0)
    if sent:
        text = "Спасибо, жалоба отправлена. Если понадобится уточнение, суперадмин свяжется с вами."
    else:
        text = "Не получилось отправить жалобу суперадмину. Попробуйте позже."
    if edit_message is not None:
        with contextlib.suppress(Exception):
            await edit_message.edit_text(text, reply_markup=None)
        await bot.send_message(chat_id, "Главное меню:", reply_markup=await user_menu_for(user_id))
    else:
        await bot.send_message(chat_id, text, reply_markup=await user_menu_for(user_id))

async def _finish_report(message: Message, state: FSMContext, report_text: str) -> None:
    await _finish_report_for_user(message.chat.id, message.from_user, state, report_text)

@dp.message(Command(commands=["report"]))
@dp.message(F.text == "📝 Жалоба")
async def report_start(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return
    await state.clear()
    await _remove_reply_keyboard_silently(message.chat.id)
    await message.answer("На что жалоба?", reply_markup=build_report_category_keyboard())

@dp.callback_query(F.data == "report:cancel")
async def report_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    if callback.message:
        await callback.message.edit_text("Жалоба отменена.", reply_markup=None)
        await callback.message.answer("Главное меню:", reply_markup=await user_menu_for(callback.from_user.id))

@dp.callback_query(F.data.in_({"report:mnemosyne", "report:chronos", "report:other"}))
async def report_category(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if not callback.message:
        return
    if callback.data == "report:mnemosyne":
        await state.set_state(ReportFlow.waiting_mnemosyne_post_id)
        await state.update_data(report_type="mnemosyne")
        await callback.message.edit_text(
            "Отправьте ID вашего поста, который бот пометил как повторку.\n\n"
            "Подойдут форматы: 999, #id999, id999.",
            reply_markup=build_report_cancel_keyboard(),
        )
        return
    report_type = "chronos" if callback.data == "report:chronos" else "other"
    await state.set_state(ReportFlow.waiting_text)
    await state.update_data(report_type=report_type)
    if report_type == "chronos":
        prompt = (
            "Опишите жалобу, мнение или идею по Хроносу — планировщику отложки.\n\n"
            "Можно одним сообщением."
        )
    else:
        prompt = "Опишите жалобу на другое, связанное с ботом. Можно одним сообщением."
    await callback.message.edit_text(prompt, reply_markup=build_report_cancel_keyboard())

@dp.message(ReportFlow.waiting_mnemosyne_post_id)
async def report_mnemosyne_post_id(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return
    post_id = _parse_report_post_id(message.text)
    if post_id is None:
        await message.answer("Нужен номер поста. Пример: 999, #id999 или id999.")
        return
    user = await db.get_user_by_tg(message.from_user.id)
    if not user:
        await state.clear()
        await message.answer("Сначала нажмите /start.", reply_markup=await user_menu_for(message.from_user.id))
        return
    post = await db.get_post(post_id)
    if not post:
        await message.answer("Пост не найден. Проверьте номер и отправьте ещё раз.")
        return
    if int(post["user_id"]) != int(user["id"]):
        await message.answer("Этот пост не принадлежит вам. Отправьте ID своего поста.")
        return
    duplicate_info = post["duplicate_info"] or ""
    if "возможный повтор" not in duplicate_info.lower():
        await message.answer(
            "У этого поста нет отметки о возможной повторке. Отправьте ID поста, который бот пометил как дубль."
        )
        return
    await state.update_data(report_post_id=post_id)
    await state.set_state(ReportFlow.waiting_mnemosyne_text)
    await message.answer(
        "Пост нашёл. Если хотите, добавьте комментарий к жалобе одним сообщением. "
        "Можно описать, почему это не повторка.\n\n"
        "Если добавить нечего, нажмите кнопку ниже.",
        reply_markup=build_report_optional_text_keyboard(),
    )

@dp.callback_query(ReportFlow.waiting_mnemosyne_text, F.data == "report:mnemosyne:no_text")
async def report_mnemosyne_no_text(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    post_id = int(data.get("report_post_id") or 0)
    post = await db.get_post(post_id)
    if not post:
        await state.clear()
        if callback.message:
            await callback.message.edit_text("Пост не найден, жалоба не отправлена.", reply_markup=None)
        return
    report_text = await _build_mnemosyne_report_text(callback.from_user, post, None)
    if callback.message:
        await _finish_report_for_user(
            callback.message.chat.id,
            callback.from_user,
            state,
            report_text,
            edit_message=callback.message,
        )

@dp.message(ReportFlow.waiting_mnemosyne_text)
async def report_mnemosyne_text(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return
    comment = (message.text or "").strip()
    data = await state.get_data()
    post_id = int(data.get("report_post_id") or 0)
    post = await db.get_post(post_id)
    if not post:
        await state.clear()
        await message.answer("Пост не найден, жалоба не отправлена.", reply_markup=await user_menu_for(message.from_user.id))
        return
    report_text = await _build_mnemosyne_report_text(message.from_user, post, comment)
    await _finish_report(message, state, report_text)

@dp.message(ReportFlow.waiting_text)
async def report_text(message: Message, state: FSMContext):
    if message.chat.type != "private":
        return
    text = (message.text or "").strip()
    if not text:
        await message.answer("Отправьте текст жалобы одним сообщением.")
        return
    data = await state.get_data()
    report_type = str(data.get("report_type") or "other")
    report_text = _build_text_report_text(message.from_user, report_type, text)
    await _finish_report(message, state, report_text)

@dp.message(SubmissionFlow.waiting_content)
async def capture_content(message: Message, state: FSMContext, album: Optional[List[Message]] = None):
    if message.chat.type != "private":
        return
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Главное меню:", reply_markup=await user_menu_for(message.from_user.id))
        return
    content = normalize_message_content(message, album=album)
    if not content:
        await message.answer("Не удалось понять контент. Пришлите текст, фото, видео или альбом.", reply_markup=SUBMIT_CANCEL_KB)
        return
    user = await db.get_user_by_tg(message.from_user.id)
    hashtag = user["hashtag"] if user and user["hashtag"] else ""
    text_body = (content.caption or "").strip()
    text_limit = max_post_text_len(content, hashtag)
    if text_body and len(text_body) > text_limit:
        if content.kind == "text":
            await message.answer(
                f"Слишком длинный текстовый пост. Сейчас можно максимум {text_limit} символов текста с учетом хэштега и ссылки, которые бот добавляет при публикации.",
                reply_markup=SUBMIT_CANCEL_KB,
            )
        else:
            await message.answer(
                f"Слишком много текста для поста с медиа. В Telegram подпись к фото или видео ограничена {MAX_MEDIA_POST_LEN} символами после оформления, поэтому сейчас можно максимум {text_limit} символов текста.",
                reply_markup=SUBMIT_CANCEL_KB,
            )
        return

    draft_raw = json.dumps(content.__dict__)
    await state.update_data(draft=draft_raw)
    await state.set_state(SubmissionFlow.confirm_content)
    confirm_markup = build_submit_confirm_keyboard()
    is_admin_sender = await is_admin(message.from_user.id)
    if content_has_images(content) or content_has_videos(content):
        token = str(time.monotonic_ns())
        fast_matches: List[Dict[str, Any]] = []
        try:
            fast_matches.extend(await detect_duplicate_images_fast(content))
        except Exception as e:
            logger.warning("Fast duplicate image precheck failed: %s", e)
        try:
            fast_matches.extend(await detect_duplicate_videos_fast(content))
        except Exception as e:
            logger.warning("Fast duplicate video precheck failed: %s", e)

        deep_task = asyncio.create_task(compute_duplicate_result_deep(content))
        control_message = await _send_submit_control_message(
            message,
            _format_submit_duplicate_scan_progress(time.monotonic(), fast_matches),
        )
        progress_task = asyncio.create_task(_animate_submit_duplicate_scan(control_message, deep_task, fast_matches))
        deep_done = True
        deep_failed = False
        deep_result: Optional[Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]] = None

        try:
            deep_result = await deep_task
        except asyncio.CancelledError:
            progress_task.cancel()
            raise
        except Exception as e:
            deep_failed = True
            logger.warning("Deep duplicate precheck failed before confirm for token %s: %s", token, e)
        finally:
            progress_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await progress_task

        merged_matches = list(fast_matches)
        if deep_result:
            merged_matches.extend(deep_result[2])
        duplicate_block = _build_admin_duplicate_block(merged_matches, admin_view=is_admin_sender)
        base_text = _submit_confirm_base_text(duplicate_block, scan_failed=deep_failed, media_checked=True)
        rendered_text = _compose_admin_submit_message(base_text, duplicate_block)
        confirm_message = await _edit_submit_control_to_confirm(control_message, rendered_text, confirm_markup)
        admin_duplicate_sessions[token] = {
            "user_id": message.from_user.id,
            "chat_id": message.chat.id,
            "message_id": confirm_message.message_id,
            "draft_raw": draft_raw,
            "base_text": base_text,
            "duplicate_block": duplicate_block,
            "reply_markup": confirm_markup,
            "rendered_text": rendered_text,
            "rendered_has_markup": True,
            "fast_matches": fast_matches,
            "admin_view": is_admin_sender,
            "deep_task": deep_task,
            "deep_done": deep_done,
            "deep_failed": deep_failed,
            "deep_result": deep_result,
            "post_id": None,
            "saved_to_db": False,
            "confirmed": False,
            "lock": asyncio.Lock(),
        }
        await state.update_data(admin_duplicate_session_token=token)
        return

    await state.update_data(admin_duplicate_session_token=None)
    confirm_text = _submit_confirm_base_text(None, media_checked=False)
    control_message = await _send_submit_control_message(message, confirm_text)
    await _edit_submit_control_to_confirm(control_message, confirm_text, confirm_markup)

@dp.callback_query(SubmissionFlow.confirm_content, F.data.in_(["confirm_send", "cancel_send"]))
async def confirm_send(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()
    admin_session_token = data.get("admin_duplicate_session_token")
    if callback.data == "cancel_send":
        await _cancel_admin_duplicate_session(admin_session_token)
        await state.clear()
        await callback.message.edit_text("Отменено.", reply_markup=None)
        await callback.message.answer("Главное меню:", reply_markup=await user_menu_for(callback.from_user.id))
        return
    draft_raw = data.get("draft")
    # всегда проверяем свежий статус админа даже если был сохранён ранее
    is_admin_sender = await is_admin(callback.from_user.id)
    if not draft_raw:
        await _cancel_admin_duplicate_session(admin_session_token)
        await callback.message.edit_text("Нет подготовленного поста.")
        await state.clear()
        return
    draft_dict = json.loads(draft_raw)
    content = DraftContent(**draft_dict)
    user = await db.get_user_by_tg(callback.from_user.id)
    if not user or not user["hashtag"]:
        await _cancel_admin_duplicate_session(admin_session_token)
        await callback.message.edit_text("Сначала задайте хэштег.")
        await state.clear()
        return
    text_body = (content.caption or "").strip()
    text_limit = max_post_text_len(content, user["hashtag"] or "")
    if text_body and len(text_body) > text_limit:
        if content.kind == "text":
            msg = (
                f"Пост слишком длинный. Сейчас можно максимум {text_limit} символов текста "
                "с учетом хэштега и ссылки при публикации."
            )
        else:
            msg = (
                f"Слишком много текста для поста с медиа. Сейчас можно максимум {text_limit} символов "
                f"текста, потому что Telegram ограничивает подпись {MAX_MEDIA_POST_LEN} символами после оформления."
            )
        await _cancel_admin_duplicate_session(admin_session_token)
        await callback.message.edit_text(msg, reply_markup=None)
        await state.set_state(SubmissionFlow.waiting_content)
        await state.update_data(is_admin=is_admin_sender, admin_duplicate_session_token=None)
        await callback.message.answer(
            "Отправь пост заново в укороченном виде или нажми Отмена.",
            reply_markup=SUBMIT_CANCEL_KB,
        )
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
    admin_session = admin_duplicate_sessions.get(admin_session_token) if admin_session_token else None
    if admin_session and admin_session.get("draft_raw") != draft_raw:
        admin_session = None

    dup_fast_matches: List[Dict[str, Any]] = []
    try:
        if admin_session is None:
            dup_fast_matches.extend(await detect_duplicate_images_fast(content))
    except Exception as e:
        logger.warning("Fast duplicate image check failed: %s", e)
    try:
        if admin_session is None:
            dup_fast_matches.extend(await detect_duplicate_videos_fast(content))
    except Exception as e:
        logger.warning("Fast duplicate video check failed: %s", e)

    dup_info: Optional[str] = None
    deep_task: Optional[asyncio.Task[Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]]] = None
    deep_pending = False
    if admin_session is not None:
        admin_session["post_id"] = post_id
        deep_task = admin_session.get("deep_task")
        deep_result = admin_session.get("deep_result")
        dup_fast_matches = list(admin_session.get("fast_matches") or [])
        if deep_result is not None:
            image_fps, video_fps, dup_deep_matches = deep_result
            if admin_session.get("deep_failed") and not (dup_fast_matches or dup_deep_matches):
                dup_info = "Повторки: ошибка проверки"
                with contextlib.suppress(Exception):
                    await db.set_post_duplicate_info(post_id, dup_info)
            else:
                await _persist_duplicate_result_for_post(
                    post_id,
                    image_fps,
                    video_fps,
                    dup_deep_matches,
                    fallback_matches=dup_fast_matches,
                )
                dup_info = format_duplicate_info(dup_fast_matches + dup_deep_matches, always_show=True)
            admin_session["saved_to_db"] = True
        elif admin_session.get("deep_failed"):
            if dup_fast_matches:
                dup_info = format_duplicate_info(dup_fast_matches, always_show=True)
            else:
                dup_info = "Повторки: ошибка проверки"
            with contextlib.suppress(Exception):
                await db.set_post_duplicate_info(post_id, dup_info)
            admin_session["saved_to_db"] = True
        else:
            deep_pending = bool(deep_task and not admin_session.get("deep_done"))
            if dup_fast_matches:
                dup_info = format_duplicate_info(dup_fast_matches, always_show=True)
                with contextlib.suppress(Exception):
                    await db.set_post_duplicate_info(post_id, dup_info)
    elif content_has_images(content) or content_has_videos(content):
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
        duplicate_commands = _build_duplicate_command_block_from_info(dup_info, admin_view=True)
        if duplicate_commands:
            caption_parts.append(escape(duplicate_commands))
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
            if admin_session is not None:
                await _cancel_admin_duplicate_session(admin_session_token)
            elif deep_task and not deep_task.done():
                deep_task.cancel()
            await callback.message.edit_text("Не удалось поставить пост в расписание.", reply_markup=None)
            await state.clear()
            return
        if admin_session is not None:
            admin_session["base_text"] = "Пост от администратора принят без голосования и поставлен в расписание"
            admin_session["reply_markup"] = None
            await _refresh_admin_duplicate_message(admin_session_token)
            admin_session["confirmed"] = True
            if admin_session.get("deep_done") and admin_session.get("saved_to_db"):
                admin_duplicate_sessions.pop(admin_session_token, None)
        else:
            await callback.message.edit_text(
                "Пост от администратора принят без голосования и поставлен в расписание",
                reply_markup=None,
            )
        if deep_pending and deep_task and admin_session is None:
            asyncio.create_task(finalize_duplicate_check_for_post(post_id, deep_task))
        await notify_user_status(post_id, "scheduled", None, scheduled_dt.isoformat())
        await state.set_state(SubmissionFlow.waiting_content)
        await state.update_data(is_admin=is_admin_sender, admin_duplicate_session_token=None)
        await callback.message.answer(
            "Можешь отправить следующий пост или нажать Отмена.",
            reply_markup=SUBMIT_CANCEL_KB,
        )
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
        if admin_session is not None:
            await _cancel_admin_duplicate_session(admin_session_token)
        elif deep_task and not deep_task.done():
            deep_task.cancel()
        await callback.message.edit_text("Не удалось отправить в админ-чат.")
        await state.clear()
        return

    if deep_pending and deep_task:
        asyncio.create_task(finalize_duplicate_check_for_post(post_id, deep_task))
    elif admin_session is not None:
        admin_session["confirmed"] = True
        if admin_session.get("saved_to_db"):
            admin_duplicate_sessions.pop(admin_session_token, None)

    await state.set_state(SubmissionFlow.waiting_content)
    await state.update_data(is_admin=is_admin_sender, admin_duplicate_session_token=None)
    await callback.message.answer(
        "Ваш пост поступил, спасибо вам за предложку!\n"
        "Пожалуйста, ожидайте решение демократичной администрации о публикации\n"
        f"ID поста - #id{post_id}\n"
        "Может ещё что-нибудь скинешь?\n"
        "🥺🥺🥺",
        reply_markup=SUBMIT_CANCEL_KB,
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
        try:
            await bot.send_message(chat_id, text)
        except Exception as e:
            logger.warning("Failed to notify user %s about reject: %s", chat_id, e)
    elif status == "scheduled":
        when = format_time(datetime.fromisoformat(scheduled_at)) if scheduled_at else "скоро"
        try:
            await bot.send_message(
                chat_id,
                "Ваш пост приняли!\n"
                f"ID поста - #id{post_id}\n"
                f"Планируемое время публикации:\n{when}\n"
                "Спасибо большое за ваш вклад.",
            )
        except Exception as e:
            logger.warning("Failed to notify user %s about schedule: %s", chat_id, e)
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


@dp.message(F.is_automatic_forward == True)
async def catch_discussion_forward(message: Message):
    if DISCUSSION_CHAT_ID == 0 or message.chat.id not in {DISCUSSION_CHAT_ID, ADMIN_CHAT_ID}:
        return
    orig_id = message.forward_from_message_id
    try:
        if orig_id is None and message.forward_origin:
            orig_id = getattr(message.forward_origin, "message_id", None)
    except Exception:
        pass
    # также ловим сообщения присланные самим каналом как sender_chat
    if orig_id is None and getattr(message.sender_chat, "id", None) == CHANNEL_ID:
        orig_id = getattr(message, "forward_from_message_id", None)
    if orig_id is not None:
        discussion_map[orig_id] = message.message_id
        waiters = discussion_waiters.pop(orig_id, [])
        for fut in waiters:
            if not fut.done():
                fut.set_result(message.message_id)
    # запоминаем последний пост канала в админ чате
    last_forward_message[message.chat.id] = message.message_id
    last_channel_message[message.chat.id] = message.message_id


@dp.message()
async def catch_forward_by_chat(message: Message):
    if DISCUSSION_CHAT_ID == 0 or message.chat.id not in {DISCUSSION_CHAT_ID, ADMIN_CHAT_ID}:
        return
    fwd_chat = getattr(message.forward_from_chat, "id", None)
    sender_chat = getattr(message.sender_chat, "id", None)
    if fwd_chat != CHANNEL_ID and sender_chat != CHANNEL_ID:
        return
    orig_id = getattr(message, "forward_from_message_id", None)
    if orig_id is None and message.forward_origin:
        orig_id = getattr(message.forward_origin, "message_id", None)
    if orig_id is None and sender_chat == CHANNEL_ID:
        orig_id = getattr(message, "forward_from_message_id", None)
    if orig_id is not None:
        discussion_map[orig_id] = message.message_id
        waiters = discussion_waiters.pop(orig_id, [])
        for fut in waiters:
            if not fut.done():
                fut.set_result(message.message_id)
    last_forward_message[message.chat.id] = message.message_id
    last_channel_message[message.chat.id] = message.message_id


@dp.message()
async def catch_prompt_message(message: Message):
    if message.chat.id not in {DISCUSSION_CHAT_ID, ADMIN_CHAT_ID}:
        return
    text = (message.text or "").strip().lower()
    if "скинуть мем" in text:
        last_prompt_message[message.chat.id] = message.message_id


@dp.message()
async def remember_last_admin_message(message: Message):
    # фиксируем только сообщения от канала/форварды канала, чтобы потом отвечать именно на них
    if message.chat.id != ADMIN_CHAT_ID:
        return
    if getattr(message.sender_chat, "id", None) == CHANNEL_ID:
        last_channel_message[ADMIN_CHAT_ID] = message.message_id
    fwd_chat = getattr(message.forward_from_chat, "id", None)
    if fwd_chat == CHANNEL_ID:
        last_channel_message[ADMIN_CHAT_ID] = message.message_id

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

