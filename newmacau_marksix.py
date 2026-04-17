#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
澳门六合彩预测工具 - 稳定版（基于最近3期）
支持自动迁移数据库表结构
"""

import argparse
import csv
import json
import re
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.request import Request, urlopen
from urllib.error import URLError

# -------------------- 常量 --------------------
SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH_DEFAULT = str(SCRIPT_DIR / "newmacau_marksix.db")
CSV_PATH_DEFAULT = str(SCRIPT_DIR / "NewMacau_Mark_Six.csv")
MACAU_API_URL = "https://marksix6.net/index.php?api=1"

ALL_NUMBERS = list(range(1, 50))
STRATEGY_LABELS = {
    "balanced_v1": "组合策略",
    "hot_v1": "热号策略",
    "cold_rebound_v1": "冷号回补",
    "momentum_v1": "近期动量",
    "ensemble_v2": "集成投票",
    "pattern_mined_v1": "规律挖掘",
}
STRATEGY_IDS = ["balanced_v1", "hot_v1", "cold_rebound_v1", "momentum_v1", "ensemble_v2", "pattern_mined_v1"]

ZODIAC_MAP = {
    "鼠": [7,19,31,43], "牛": [6,18,30,42], "虎": [5,17,29,41], "兔": [4,16,28,40],
    "龙": [3,15,27,39], "蛇": [2,14,26,38], "马": [1,13,25,37,49], "羊": [12,24,36,48],
    "猴": [11,23,35,47], "鸡": [10,22,34,46], "狗": [9,21,33,45], "猪": [8,20,32,44],
}

# -------------------- 数据结构 --------------------
@dataclass
class DrawRecord:
    issue_no: str
    draw_date: str
    numbers: List[int]
    special_number: int

@dataclass
class StrategyScore:
    main_picks: List[int]
    special_pick: int
    confidence: float
    raw_scores: Dict[int, float] = field(default_factory=dict)

# -------------------- 工具函数 --------------------
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS draws (
            issue_no TEXT PRIMARY KEY,
            draw_date TEXT NOT NULL,
            numbers_json TEXT NOT NULL,
            special_number INTEGER NOT NULL,
            source TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS prediction_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_no TEXT NOT NULL,
            strategy TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            hit_count INTEGER,
            hit_rate REAL,
            hit_count_10 INTEGER,
            hit_rate_10 REAL,
            hit_count_14 INTEGER,
            hit_rate_14 REAL,
            hit_count_20 INTEGER,
            hit_rate_20 REAL,
            special_hit INTEGER,
            created_at TEXT NOT NULL,
            reviewed_at TEXT,
            UNIQUE(issue_no, strategy)
        );
        CREATE TABLE IF NOT EXISTS prediction_picks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            pick_type TEXT NOT NULL DEFAULT 'MAIN',
            number INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            score REAL NOT NULL,
            reason TEXT NOT NULL,
            UNIQUE(run_id, number),
            FOREIGN KEY(run_id) REFERENCES prediction_runs(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS prediction_pools (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            pool_size INTEGER NOT NULL,
            numbers_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(run_id, pool_size),
            FOREIGN KEY(run_id) REFERENCES prediction_runs(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS model_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
    """)
    _ensure_columns(conn)
    conn.commit()

def _ensure_columns(conn: sqlite3.Connection) -> None:
    """确保所有必要的列存在，自动添加缺失的列"""
    # 检查 draws 表
    draws_cols = {r[1] for r in conn.execute("PRAGMA table_info(draws)").fetchall()}
    for col in ["sum_value", "odd_count", "big_count", "consec_pairs", "zodiac_json"]:
        if col not in draws_cols:
            col_type = "TEXT" if col == "zodiac_json" else "INTEGER"
            conn.execute(f"ALTER TABLE draws ADD COLUMN {col} {col_type}")
    # 检查 prediction_runs 表
    runs_cols = {r[1] for r in conn.execute("PRAGMA table_info(prediction_runs)").fetchall()}
    # 原有列: id, issue_no, strategy, status, hit_count, hit_rate, created_at, reviewed_at
    # 需要添加的列
    needed_cols = {
        "hit_count_10": "INTEGER",
        "hit_rate_10": "REAL",
        "hit_count_14": "INTEGER",
        "hit_rate_14": "REAL",
        "hit_count_20": "INTEGER",
        "hit_rate_20": "REAL",
        "special_hit": "INTEGER",
        "numbers_json": "TEXT",
        "special_number": "INTEGER",
        "confidence": "REAL",
    }
    for col, col_type in needed_cols.items():
        if col not in runs_cols:
            conn.execute(f"ALTER TABLE prediction_runs ADD COLUMN {col} {col_type}")
    # 检查 prediction_picks 表
    picks_cols = {r[1] for r in conn.execute("PRAGMA table_info(prediction_picks)").fetchall()}
    if "pick_type" not in picks_cols:
        conn.execute("ALTER TABLE prediction_picks ADD COLUMN pick_type TEXT NOT NULL DEFAULT 'MAIN'")
    # 检查 prediction_pools 表
    pools_cols = {r[1] for r in conn.execute("PRAGMA table_info(prediction_pools)").fetchall()}
    if "created_at" not in pools_cols:
        conn.execute("ALTER TABLE prediction_pools ADD COLUMN created_at TEXT")

# -------------------- 数据获取（带重试）--------------------
def fetch_macau_records(retry=3, timeout=60) -> List[DrawRecord]:
    for attempt in range(1, retry+1):
        try:
            req = Request(MACAU_API_URL, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            for item in data.get("lottery_data", []):
                if item.get("name") == "新澳门彩":
                    macau = item
                    break
            else:
                raise RuntimeError("未找到新澳门彩数据")
            records = []
            for line in macau.get("history", []):
                m = re.match(r"(\d{7})\s*期[：:]\s*([\d,]+)", line)
                if m:
                    nums = [int(x) for x in m.group(2).split(",")]
                    if len(nums) >= 7:
                        records.append(DrawRecord(
                            issue_no=f"{m.group(1)[:4]}/{m.group(1)[4:]}",
                            draw_date=datetime.now().strftime("%Y-%m-%d"),
                            numbers=nums[:6],
                            special_number=nums[6]
                        ))
            if not records:
                expect = str(macau.get("expect", ""))
                code = macau.get("openCode", "")
                if code:
                    nums = [int(x) for x in code.split(",")]
                    if len(nums) >= 7:
                        records.append(DrawRecord(
                            issue_no=f"{expect[:4]}/{expect[4:]}",
                            draw_date=macau.get("openTime", "")[:10],
                            numbers=nums[:6],
                            special_number=nums[6]
                        ))
            if records:
                print(f"成功获取 {len(records)} 条记录 (尝试 {attempt}/{retry})")
                return records
        except Exception as e:
            print(f"API 请求失败 (尝试 {attempt}/{retry}): {e}")
            if attempt < retry:
                time.sleep(5)
    # 回退到 CSV
    if Path(CSV_PATH_DEFAULT).exists():
        records = parse_draw_csv(CSV_PATH_DEFAULT)
        if records:
            print(f"从 CSV 导入 {len(records)} 条记录")
            return records
    raise RuntimeError("无法获取澳门彩数据，请检查网络或提供 CSV 文件")

def parse_draw_csv(csv_path: str) -> List[DrawRecord]:
    records = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            issue = row.get("期号") or row.get("issue_no")
            date_str = row.get("日期") or row.get("draw_date")
            nums_str = row.get("开奖号码") or row.get("numbers")
            special_str = row.get("特别号码") or row.get("special_number")
            if not issue or not nums_str:
                continue
            nums = [int(x) for x in re.findall(r"\d+", nums_str)]
            if len(nums) >= 7:
                records.append(DrawRecord(
                    issue_no=issue,
                    draw_date=date_str or "",
                    numbers=nums[:6],
                    special_number=nums[6]
                ))
    return records

def upsert_draw(conn, record, source):
    now = utc_now()
    if conn.execute("SELECT 1 FROM draws WHERE issue_no=?", (record.issue_no,)).fetchone():
        conn.execute("""UPDATE draws SET draw_date=?, numbers_json=?, special_number=?, source=?, updated_at=?
                        WHERE issue_no=?""",
                     (record.draw_date, json.dumps(record.numbers), record.special_number, source, now, record.issue_no))
        return "updated"
    else:
        conn.execute("""INSERT INTO draws (issue_no, draw_date, numbers_json, special_number, source, created_at, updated_at)
                        VALUES (?,?,?,?,?,?,?)""",
                     (record.issue_no, record.draw_date, json.dumps(record.numbers), record.special_number, source, now, now))
        return "inserted"

def sync_from_records(conn, records, source):
    ins = upd = 0
    for r in records:
        res = upsert_draw(conn, r, source)
        if res == "inserted": ins += 1
        else: upd += 1
    conn.commit()
    return len(records), ins, upd

# -------------------- 特征计算 --------------------
def load_recent_draws(conn, limit=3):
    rows = conn.execute("SELECT numbers_json FROM draws ORDER BY draw_date DESC, issue_no DESC LIMIT ?", (limit,)).fetchall()
    return [json.loads(r[0]) for r in rows]

def _normalize(d):
    vals = list(d.values())
    mn, mx = min(vals), max(vals)
    if mx == mn: return {k:0.0 for k in d}
    return {k:(v-mn)/(mx-mn) for k,v in d.items()}

def _freq_map(draws):
    freq = {n:0.0 for n in ALL_NUMBERS}
    for d in draws:
        for n in d: freq[n] += 1.0
    return freq

def _omission_map(draws):
    omission = {n:float(len(draws)+1) for n in ALL_NUMBERS}
    for i,d in enumerate(draws):
        for n in d:
            omission[n] = min(omission[n], float(i+1))
    return omission

def _momentum_map(draws):
    mom = {n:0.0 for n in ALL_NUMBERS}
    for i,d in enumerate(draws):
        w = 1.0/(1.0+i)
        for n in d: mom[n] += w
    return mom

def calculate_pair_lift(draws):
    pair = Counter()
    single = Counter()
    for d in draws:
        for n in d: single[n] += 1
        for a,b in combinations(sorted(d),2):
            pair[(a,b)] += 1
    total = len(draws)
    lift = {}
    for (a,b), cnt in pair.items():
        exp = (single[a]/total)*(single[b]/total)*total
        if exp>0: lift[(a,b)] = cnt/exp
    return lift

def _pick_top_six(scores, reason):
    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    picked = []
    for n,s in ranked:
        if len(picked)==6: break
        proposal = [pn for pn,_ in picked] + [n]
        odd = sum(1 for x in proposal if x%2)
        if len(proposal)>=4 and (odd==0 or odd==len(proposal)): continue
        zones = [(x-1)//10 for x in proposal]
        if max(Counter(zones).values())>=4: continue
        picked.append((n,s))
    while len(picked)<6:
        for n,s in ranked:
            if n not in [pn for pn,_ in picked]:
                picked.append((n,s))
                break
    total = sum(n for n,_ in picked[:6])
    if total<95 or total>205:
        for i in range(5,-1,-1):
            for alt_n, alt_s in ranked:
                if alt_n in [pn for pn,_ in picked]: continue
                cand = [pn for pn,_ in picked]
                cand[i] = alt_n
                if 95 <= sum(cand) <= 205:
                    picked[i] = (alt_n, alt_s)
                    break
            else: continue
            break
    return [(n, idx+1, s, f"{reason} score={s:.4f}") for idx,(n,s) in enumerate(picked)]

def _apply_weight_config(draws, config, reason):
    window = draws[:max(3, int(config.get("window",3)))]
    freq = _normalize(_freq_map(window))
    omission = _normalize({n:1.0/(_omission_map(window)[n]+1) for n in ALL_NUMBERS})
    mom = _normalize(_momentum_map(window))
    scores = {n: freq[n]*config.get("w_freq",0.45) + omission[n]*config.get("w_omit",0.35) + mom[n]*config.get("w_mom",0.20) for n in ALL_NUMBERS}
    main_picks = _pick_top_six(scores, reason)
    main_set = {n for n,_,_,_ in main_picks}
    special = max((n for n in ALL_NUMBERS if n not in main_set), key=lambda n: scores[n])
    return main_picks, special, 0.0, scores

def generate_strategy(draws, strategy, mined_cfg=None):
    cfg = {"window":3.0, "w_freq":0.4, "w_omit":0.3, "w_mom":0.2}
    if strategy == "hot_v1":
        cfg = {"window":3.0, "w_freq":0.8, "w_omit":0.0, "w_mom":0.2}
    elif strategy == "cold_rebound_v1":
        cfg = {"window":3.0, "w_freq":0.0, "w_omit":0.7, "w_mom":0.3}
    elif strategy == "momentum_v1":
        cfg = {"window":3.0, "w_freq":0.1, "w_omit":0.0, "w_mom":0.9}
    elif strategy == "ensemble_v2":
        return _ensemble_strategy(draws)
    elif strategy == "pattern_mined_v1":
        cfg = mined_cfg or {"window":3.0, "w_freq":0.4, "w_omit":0.3, "w_mom":0.2}
    return _apply_weight_config(draws, cfg, STRATEGY_LABELS[strategy])

def _ensemble_strategy(draws):
    scores_list = []
    for s in ["hot_v1","cold_rebound_v1","momentum_v1","balanced_v1","pattern_mined_v1"]:
        picks, _, _, raw = generate_strategy(draws, s)
        scores_list.append(raw)
    votes = {n:0.0 for n in ALL_NUMBERS}
    for sc in scores_list:
        for rank,(n,_) in enumerate(sorted(sc.items(), key=lambda x:x[1], reverse=True)):
            votes[n] += 49-rank
    maxv = max(votes.values())
    norm = {n:v/maxv for n,v in votes.items()}
    main = _pick_top_six(norm, "集成投票")
    main_set = {n for n,_,_,_ in main}
    special = max((n for n in ALL_NUMBERS if n not in main_set), key=lambda n: norm[n])
    return main, special, sum(norm[n] for n,_,_,_ in main)/6, norm

def generate_predictions(conn, issue_no=None):
    row = conn.execute("SELECT issue_no FROM draws ORDER BY draw_date DESC LIMIT 1").fetchone()
    if not row: raise RuntimeError("无开奖数据，请先同步")
    target = issue_no or next_issue(row["issue_no"])
    draws = load_recent_draws(conn, 3)
    if len(draws)<3: raise RuntimeError("需要至少3期数据")
    for strategy in STRATEGY_IDS:
        picks, special, conf, _ = generate_strategy(draws, strategy)
        # 保存预测
        conn.execute("""INSERT OR REPLACE INTO prediction_runs 
                        (issue_no, strategy, numbers_json, special_number, confidence, status, created_at)
                        VALUES (?,?,?,?,?,'PENDING',?)""",
                     (target, strategy, json.dumps([n for n,_,_,_ in picks]), special, conf, utc_now()))
        # 保存 picks
        run_id = conn.execute("SELECT id FROM prediction_runs WHERE issue_no=? AND strategy=?", (target, strategy)).fetchone()[0]
        conn.execute("DELETE FROM prediction_picks WHERE run_id=?", (run_id,))
        for idx, (n, rank, score, reason) in enumerate(picks):
            conn.execute("INSERT INTO prediction_picks (run_id, pick_type, number, rank, score, reason) VALUES (?, 'MAIN', ?, ?, ?, ?)",
                         (run_id, n, rank, score, reason))
        conn.execute("INSERT INTO prediction_picks (run_id, pick_type, number, rank, score, reason) VALUES (?, 'SPECIAL', ?, 1, ?, '特别号候选')",
                     (run_id, special, conf))
        # 保存 pools（简化，只存6码池）
        conn.execute("DELETE FROM prediction_pools WHERE run_id=?", (run_id,))
        conn.execute("INSERT INTO prediction_pools (run_id, pool_size, numbers_json, created_at) VALUES (?, 6, ?, ?)",
                     (run_id, json.dumps([n for n,_,_,_ in picks]), utc_now()))
    conn.commit()
    return target

def next_issue(issue):
    y,s = issue.split("/")
    return f"{y}/{str(int(s)+1).zfill(3)}"

def review_latest(conn):
    row = conn.execute("SELECT issue_no, numbers_json, special_number FROM draws ORDER BY draw_date DESC LIMIT 1").fetchone()
    if not row: return 0
    win_main = set(json.loads(row["numbers_json"]))
    win_special = row["special_number"]
    runs = conn.execute("SELECT id, numbers_json, special_number FROM prediction_runs WHERE issue_no=? AND status='PENDING'", (row["issue_no"],)).fetchall()
    cnt=0
    for r in runs:
        pred_main = set(json.loads(r["numbers_json"]))
        hit = len(pred_main & win_main)
        hit_rate = hit/6.0
        special_hit = 1 if r["special_number"]==win_special else 0
        conn.execute("""UPDATE prediction_runs 
                        SET status='REVIEWED', hit_count=?, hit_rate=?, special_hit=?, reviewed_at=?
                        WHERE id=?""",
                     (hit, hit_rate, special_hit, utc_now(), r["id"]))
        cnt+=1
    conn.commit()
    return cnt

def backfill_missing_special_picks(conn):
    # 确保每个PENDING预测都有SPECIAL记录（但上面生成时已经插入，此函数可留空或实现）
    pass

def print_dashboard(conn):
    latest = conn.execute("SELECT issue_no, draw_date, numbers_json, special_number FROM draws ORDER BY draw_date DESC LIMIT 1").fetchone()
    if latest:
        nums = " ".join(f"{n:02d}" for n in json.loads(latest["numbers_json"]))
        print(f"最新开奖: {latest['issue_no']} {latest['draw_date']} | 主号: {nums} | 特别号: {latest['special_number']:02d}")
    else:
        print("暂无开奖数据。")
    pending = conn.execute("SELECT issue_no, strategy, numbers_json, special_number FROM prediction_runs WHERE status='PENDING' ORDER BY strategy").fetchall()
    if pending:
        print("\n本期多策略推荐:")
        for p in pending:
            nums = json.loads(p["numbers_json"])
            print(f"  [{p['issue_no']}] {STRATEGY_LABELS.get(p['strategy'], p['strategy'])}: {' '.join(f'{n:02d}' for n in nums)} | 特别号: {p['special_number']:02d}")
    else:
        print("\n暂无待开奖预测，请先运行 predict")
    # 最终推荐使用 ensemble
    ens = conn.execute("SELECT numbers_json, special_number FROM prediction_runs WHERE strategy='ensemble_v2' AND status='PENDING'").fetchone()
    if ens:
        main6 = json.loads(ens["numbers_json"])
        special = ens["special_number"]
        issue = pending[0]['issue_no'] if pending else "?"
        print("\n" + "="*50)
        print(f"【最终推荐 - 期号 {issue}】")
        print(f"  6号池 : {' '.join(f'{n:02d}' for n in main6)} | 特别号: {special:02d}")
        print("="*50)

# -------------------- 命令行接口 --------------------
def cmd_bootstrap(args):
    conn = connect_db(args.db)
    init_db(conn)
    records = fetch_macau_records()
    total, ins, upd = sync_from_records(conn, records, "api")
    print(f"引导完成: 总计 {total} 期, 新增 {ins}, 更新 {upd}")
    generate_predictions(conn)
    conn.close()

def cmd_sync(args):
    conn = connect_db(args.db)
    init_db(conn)
    records = fetch_macau_records()
    total, ins, upd = sync_from_records(conn, records, "api")
    print(f"同步完成: 总计 {total} 期, 新增 {ins}, 更新 {upd}")
    reviewed = review_latest(conn)
    if reviewed:
        print(f"已复盘 {reviewed} 期")
    generate_predictions(conn)
    conn.close()

def cmd_predict(args):
    conn = connect_db(args.db)
    init_db(conn)
    issue = generate_predictions(conn, args.issue)
    print(f"已生成 {issue} 期预测")
    conn.close()

def cmd_show(args):
    conn = connect_db(args.db)
    init_db(conn)
    backfill_missing_special_picks(conn)
    print_dashboard(conn)
    conn.close()

def build_parser():
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=DB_PATH_DEFAULT)
    sub = p.add_subparsers(dest="command", required=True)
    p_boot = sub.add_parser("bootstrap", help="首次引导")
    p_boot.set_defaults(func=cmd_bootstrap)
    p_sync = sub.add_parser("sync", help="同步数据")
    p_sync.set_defaults(func=cmd_sync)
    p_predict = sub.add_parser("predict", help="生成预测")
    p_predict.add_argument("--issue", help="指定期号")
    p_predict.set_defaults(func=cmd_predict)
    p_show = sub.add_parser("show", help="显示推荐")
    p_show.set_defaults(func=cmd_show)
    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
