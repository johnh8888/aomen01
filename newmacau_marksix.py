#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import re
import socket
import sqlite3
import time
from urllib.error import URLError
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.request import Request, urlopen

SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH_DEFAULT = str(SCRIPT_DIR / "newmacau_marksix.db")
CSV_PATH_DEFAULT = str(SCRIPT_DIR / "NewMacau_Mark_Six.csv")

# 澳门数据源（使用 marksix6.net API 中的“新澳门彩”）
MACAU_API_URL = "https://marksix6.net/index.php?api=1"
API_TIMEOUT_DEFAULT = 20
API_RETRIES_DEFAULT = 4
API_RETRY_BACKOFF_SECONDS = 2.0

MINED_CONFIG_KEY = "mined_strategy_config_v1"
ALL_NUMBERS = list(range(1, 50))

# ==================== 【优化后常量】 ====================
FEATURE_WINDOW_DEFAULT = 10

STRATEGY_BASE_WINDOWS = {
    "hot_v1": 6,
    "momentum_v1": 7,
    "cold_rebound_v1": 13,
    "balanced_v1": 10,
    "pattern_mined_v1": 6,
    "ensemble_v2": 10,
}

WEIGHT_WINDOW_DEFAULT = 30
HEALTH_WINDOW_DEFAULT = 18
BACKTEST_ISSUES_DEFAULT = 120

# Ensemble v3.1 配置
ENSEMBLE_DIVERSITY_BONUS = 0.13

# 偏态检测阈值（已调整）
BIAS_THRESHOLD = 0.65
BIAS_ADJUSTMENT = 0.40
FORCED_BIAS_COEFFICIENT = 0.75

STRATEGY_LABELS = {
    "balanced_v1": "组合策略",
    "hot_v1": "热号策略",
    "cold_rebound_v1": "冷号回补",
    "momentum_v1": "近期动量",
    "ensemble_v2": "集成投票",
    "pattern_mined_v1": "规律挖掘",
}
STRATEGY_IDS = ["balanced_v1", "hot_v1", "cold_rebound_v1", "momentum_v1", "ensemble_v2", "pattern_mined_v1"]
SPECIAL_ANALYSIS_ORDER = ["pattern_mined_v1", "ensemble_v2", "momentum_v1", "cold_rebound_v1", "hot_v1", "balanced_v1"]

# 生肖映射（正确版本：1=马，2=蛇，3=龙，4=兔，5=虎，6=牛，7=鼠，8=猪，9=狗，10=鸡，11=猴，12=羊）
ZODIAC_MAP = {
    "马": [1, 13, 25, 37, 49],
    "蛇": [2, 14, 26, 38],
    "龙": [3, 15, 27, 39],
    "兔": [4, 16, 28, 40],
    "虎": [5, 17, 29, 41],
    "牛": [6, 18, 30, 42],
    "鼠": [7, 19, 31, 43],
    "猪": [8, 20, 32, 44],
    "狗": [9, 21, 33, 45],
    "鸡": [10, 22, 34, 46],
    "猴": [11, 23, 35, 47],
    "羊": [12, 24, 36, 48],
}

# PushPlus 配置
PUSHPLUS_TOKEN = ""
if os.environ.get("PUSHPLUS_TOKEN"):
    PUSHPLUS_TOKEN = os.environ["PUSHPLUS_TOKEN"]

_WEIGHT_PROTECTION_PRINTED: set[str] = set()
_PROTECTION_PRINT_COUNTER = 0


@dataclass
class DrawRecord:
    issue_no: str
    draw_date: str
    numbers: List[int]
    special_number: int


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn   
