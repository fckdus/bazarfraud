"""
БАЗАР Антифрод v1.4 — ядро
============================
Логика детекторов, загрузка данных, генерация отчёта.
Используется и CLI-скриптом, и Streamlit-интерфейсом.
"""

import time
import math
import logging
from urllib.parse import parse_qs, unquote
from collections import defaultdict, Counter
from typing import Optional

import requests

log = logging.getLogger("bazar_antifraud")


# ===================================================================
# 1. ЯНДЕКС ДИРЕКТ API v5
# ===================================================================

def fetch_direct_placements_single(token, login, date_from, date_to, progress_cb=None):
    url = "https://api.direct.yandex.com/json/v5/reports"
    headers = {
        "Authorization": f"Bearer {token}",
        "Client-Login": login,
        "Accept-Language": "ru",
        "processingMode": "auto",
        "returnMoneyInMicros": "false",
        "skipReportHeader": "true",
        "skipReportSummary": "true",
    }
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from, "DateTo": date_to,
                "Filter": [{"Field": "AdNetworkType", "Operator": "EQUALS", "Values": ["AD_NETWORK"]}],
            },
            "FieldNames": ["Date", "Placement", "CampaignName", "Impressions", "Clicks", "Cost", "Ctr", "BounceRate"],
            "ReportName": f"antifraud_{login}_{int(time.time())}",
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "YES",
            "IncludeDiscount": "NO",
        }
    }

    if progress_cb:
        progress_cb(f"Директ [{login}]: запрашиваю отчёт...")
    log.info(f"Запрашиваю отчёт из Директа [{login}]...")

    while True:
        resp = requests.post(url, json=body, headers=headers)
        if resp.status_code == 200:
            break
        elif resp.status_code in (201, 202):
            if progress_cb:
                progress_cb(f"Директ [{login}]: формируется, жду...")
            time.sleep(10)
        else:
            log.error(f"Ошибка Директа [{login}]: {resp.status_code}\n{resp.text}")
            return []

    lines = resp.text.strip().split("\n")
    if len(lines) < 2:
        return []

    header = lines[0].split("\t")
    rows = []
    for line in lines[1:]:
        vals = line.split("\t")
        row = dict(zip(header, vals))
        for fld in ("Impressions", "Clicks"):
            val = row.get(fld, "0")
            row[fld] = int(val) if val not in ("", "--", None) else 0
        for fld in ("Cost", "Ctr", "BounceRate"):
            val = row.get(fld, "0")
            row[fld] = float(val) if val not in ("", "--", None) else 0.0
        rows.append(row)

    log.info(f"Получено {len(rows)} строк из Директа [{login}]")
    return rows


def fetch_direct_placements(accounts, date_from, date_to, progress_cb=None):
    all_rows = []
    for account in accounts:
        rows = fetch_direct_placements_single(
            account["token"], account["login"], date_from, date_to, progress_cb
        )
        all_rows.extend(rows)
    log.info(f"Итого из Директа: {len(all_rows)} строк")
    return all_rows


# ===================================================================
# 2. APPMETRICA LOGS API
# ===================================================================

def _appmetrica_logs_request(endpoint, fields, app_id, token, date_since, date_until, progress_cb=None):
    url = f"https://api.appmetrica.yandex.ru/logs/v1/export/{endpoint}.json"
    params = {"application_id": app_id, "date_since": date_since, "date_until": date_until, "fields": fields}
    headers = {"Authorization": f"OAuth {token}"}

    if progress_cb:
        progress_cb(f"AppMetrica: загружаю {endpoint}...")
    log.info(f"Запрашиваю AppMetrica: {endpoint}...")

    while True:
        resp = requests.get(url, params=params, headers=headers)
        if resp.status_code == 200:
            break
        elif resp.status_code == 202:
            if progress_cb:
                progress_cb(f"AppMetrica: готовлю {endpoint}, жду...")
            time.sleep(15)
        else:
            log.error(f"Ошибка AppMetrica ({endpoint}): {resp.status_code}\n{resp.text}")
            return []

    data = resp.json().get("data", [])
    log.info(f"Получено {len(data)} записей из AppMetrica/{endpoint}")
    return data


def fetch_installations(app_id, token, date_since, date_until, progress_cb=None):
    fields = ",".join([
        "installation_id", "appmetrica_device_id", "tracker_name",
        "publisher_name", "click_url_parameters", "install_datetime",
        "install_timestamp", "click_timestamp",
        "city", "country_iso_code", "device_manufacturer", "device_model", "os_name",
    ])
    return _appmetrica_logs_request("installations", fields, app_id, token, date_since, date_until, progress_cb)


def fetch_events(app_id, token, date_since, date_until, progress_cb=None):
    fields = ",".join([
        "event_name", "event_datetime", "event_timestamp",
        "session_id", "installation_id", "appmetrica_device_id", "city",
    ])
    return _appmetrica_logs_request("events", fields, app_id, token, date_since, date_until, progress_cb)


# ===================================================================
# 3. ПАРСИНГ SOURCE
# ===================================================================

def extract_source(click_url_params, source_param_name):
    if not click_url_params:
        return None
    try:
        parsed = parse_qs(unquote(click_url_params))
        vals = parsed.get(source_param_name, [])
        return vals[0] if vals else None
    except Exception:
        return None


# ===================================================================
# 4. МЭТЧИНГ
# ===================================================================

def build_placement_data(direct_rows, installations, events, source_param_name, reg_event_name):
    placements = defaultdict(lambda: {
        "impressions": 0, "clicks": 0, "cost": 0.0,
        "bounce_rate_sum": 0.0, "bounce_rate_count": 0,
        "daily_clicks": defaultdict(int),
        "installs": 0, "registrations": 0,
        "devices": set(),
        "daily_installs": defaultdict(int),
        "session_events": defaultdict(list),
        "ctit_values": [],
        "device_models": [],
    })

    direct_placement_names = set()

    for row in direct_rows:
        pl = row.get("Placement", "").strip()
        if not pl:
            continue
        direct_placement_names.add(pl)
        p = placements[pl]
        p["impressions"] += row["Impressions"]
        p["clicks"] += row["Clicks"]
        p["cost"] += row["Cost"]
        if row["BounceRate"]:
            p["bounce_rate_sum"] += row["BounceRate"]
            p["bounce_rate_count"] += 1
        p["daily_clicks"][row.get("Date", "")] += row["Clicks"]

    # Строим ДВА словаря:
    # 1) device_id → source (для привязки событий к площадкам)
    # 2) device_id → True (для всех установок, даже без source)
    install_device_to_source = {}
    all_install_devices = set()

    for inst in installations:
        device_id = inst.get("appmetrica_device_id", "")
        if not device_id:
            continue

        all_install_devices.add(device_id)

        source = extract_source(inst.get("click_url_parameters", ""), source_param_name)
        if not source:
            # Не берём publisher_name как fallback для площадки —
            # это даёт ложные "площадки" вроде "Yandex.Direct"
            continue

        install_device_to_source[device_id] = source
        p = placements[source]
        p["installs"] += 1
        p["devices"].add(device_id)
        install_dt = inst.get("install_datetime", "")
        if install_dt:
            p["daily_installs"][install_dt[:10]] += 1

        click_ts = inst.get("click_timestamp")
        install_ts = inst.get("install_timestamp")
        if click_ts and install_ts:
            try:
                ctit = int(install_ts) - int(click_ts)
                if ctit >= 0:
                    p["ctit_values"].append(ctit)
            except (ValueError, TypeError):
                pass

        model = inst.get("device_model", "")
        if model:
            p["device_models"].append(model)

    # Обрабатываем события: привязываем к площадке через device_id
    for ev in events:
        device_id = ev.get("appmetrica_device_id", "")
        source = install_device_to_source.get(device_id)
        if not source:
            continue

        p = placements[source]
        event_name = ev.get("event_name", "")
        session_id = ev.get("session_id", "")

        if session_id:
            p["session_events"][session_id].append(event_name)

        if event_name == reg_event_name:
            p["registrations"] += 1

    return placements, direct_placement_names


# ===================================================================
# 5. ДЕТЕКТОРЫ
# ===================================================================

def _coeff_variation(values):
    if len(values) < 3:
        return 999.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 999.0
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return math.sqrt(variance) / mean


def detect_fraud(placements, direct_only):
    results = []

    all_crs = []
    for pl_name, p in placements.items():
        if pl_name not in direct_only:
            continue
        if p["installs"] >= 10:
            all_crs.append(p["registrations"] / p["installs"])
    global_cr_cv = _coeff_variation(all_crs) if all_crs else 999

    for pl_name, p in placements.items():
        if pl_name not in direct_only:
            continue
        if p["installs"] < 5 and p["clicks"] < 10:
            continue

        flags = {}
        scores = {}

        # #1: CR-аномалия
        cr = p["registrations"] / p["installs"] if p["installs"] > 0 else 0
        cr_flag = False
        cr_score = 0
        if cr > 0.7 and p["installs"] >= 10:
            cr_flag = True
            cr_score = 30
        if global_cr_cv < 0.05 and len(all_crs) >= 3:
            cr_flag = True
            cr_score = max(cr_score, 25)
        flags["cr_anomaly"] = cr_flag
        scores["cr_anomaly"] = cr_score

        # #2: Равномерный залив
        daily_vals = list(p["daily_installs"].values())
        daily_cv = _coeff_variation(daily_vals)
        daily_flag = daily_cv < 0.10 and len(daily_vals) >= 5
        daily_score = 25 if daily_flag else 0
        flags["even_volume"] = daily_flag
        scores["even_volume"] = daily_score

        # #3: Аномалии сессий (порог 25)
        session_counts = []
        for sid, evts in p["session_events"].items():
            sys_events = [e for e in evts if any(kw in e.lower() for kw in
                          ["launch", "start", "open", "session", "запуск", "открытие", "старт"])]
            session_counts.append(len(sys_events))
        avg_sys = sum(session_counts) / len(session_counts) if session_counts else 0
        session_flag = avg_sys > 25 and len(session_counts) >= 10
        session_score = 20 if session_flag else 0
        flags["session_anomaly"] = session_flag
        scores["session_anomaly"] = session_score

        # #4: Высокий bounce rate (усиленный)
        avg_bounce = p["bounce_rate_sum"] / p["bounce_rate_count"] if p["bounce_rate_count"] > 0 else 0
        bounce_flag = False
        bounce_score = 0
        if avg_bounce >= 95 and p["clicks"] >= 20 and p["cost"] > 3000:
            bounce_flag = True
            bounce_score = 30
        elif avg_bounce > 80 and p["clicks"] >= 20:
            bounce_flag = True
            bounce_score = 15
        flags["high_bounce"] = bounce_flag
        scores["high_bounce"] = bounce_score

        # #5: CTIT-аномалия
        ctit_vals = p["ctit_values"]
        ctit_flag = False
        ctit_score = 0
        ctit_median = 0
        ctit_pct_under_10s = 0
        ctit_pct_over_24h = 0
        if len(ctit_vals) >= 5:
            sorted_ctit = sorted(ctit_vals)
            ctit_median = sorted_ctit[len(sorted_ctit) // 2]
            under_10s = sum(1 for v in ctit_vals if v < 10)
            ctit_pct_under_10s = under_10s / len(ctit_vals)
            if ctit_pct_under_10s > 0.3:
                ctit_flag = True
                ctit_score = 25
            over_24h = sum(1 for v in ctit_vals if v > 86400)
            ctit_pct_over_24h = over_24h / len(ctit_vals)
            if ctit_pct_over_24h > 0.5:
                ctit_flag = True
                ctit_score = max(ctit_score, 20)
        flags["ctit_anomaly"] = ctit_flag
        scores["ctit_anomaly"] = ctit_score

        # #6: CVR-аномалия
        cvr = p["installs"] / p["clicks"] if p["clicks"] > 0 else 0
        cvr_flag = False
        cvr_score = 0
        if p["clicks"] >= 500 and cvr < 0.0005:
            cvr_flag = True
            cvr_score = 15
        if p["clicks"] >= 5 and p["installs"] > p["clicks"] * 5 and p["installs"] >= 20:
            cvr_flag = True
            cvr_score = max(cvr_score, 20)
        flags["cvr_anomaly"] = cvr_flag
        scores["cvr_anomaly"] = cvr_score

        # #7: Концентрация устройств
        models = p["device_models"]
        device_flag = False
        device_score = 0
        top_model_pct = 0
        if len(models) >= 10:
            counter = Counter(models)
            top_model, top_count = counter.most_common(1)[0]
            top_model_pct = top_count / len(models)
            if top_model_pct > 0.80:
                device_flag = True
                device_score = 30
            elif top_model_pct > 0.60:
                device_flag = True
                device_score = 20
        flags["device_concentration"] = device_flag
        scores["device_concentration"] = device_score

        fraud_score = sum(scores.values())
        risk_level = (
            "CRITICAL" if fraud_score >= 50 else
            "HIGH" if fraud_score >= 30 else
            "MEDIUM" if fraud_score >= 15 else
            "LOW"
        )

        results.append({
            "placement": pl_name,
            "risk_level": risk_level,
            "fraud_score": fraud_score,
            "impressions": p["impressions"],
            "clicks": p["clicks"],
            "cost": round(p["cost"], 2),
            "installs": p["installs"],
            "registrations": p["registrations"],
            "cr_install_to_reg": round(cr, 4),
            "cvr_click_to_install": round(cvr, 4) if p["clicks"] > 0 else None,
            "avg_bounce_rate": round(avg_bounce, 1),
            "daily_install_cv": round(daily_cv, 4) if daily_cv < 900 else None,
            "avg_sys_events_per_session": round(avg_sys, 2),
            "ctit_median_sec": round(ctit_median, 1) if ctit_vals else None,
            "ctit_pct_under_10s": round(ctit_pct_under_10s, 3) if ctit_vals else None,
            "ctit_pct_over_24h": round(ctit_pct_over_24h, 3) if ctit_vals else None,
            "top_device_model_pct": round(top_model_pct, 3) if models else None,
            "flag_cr_anomaly": flags["cr_anomaly"],
            "flag_even_volume": flags["even_volume"],
            "flag_session_anomaly": flags["session_anomaly"],
            "flag_high_bounce": flags["high_bounce"],
            "flag_ctit_anomaly": flags["ctit_anomaly"],
            "flag_cvr_anomaly": flags["cvr_anomaly"],
            "flag_device_concentration": flags["device_concentration"],
        })

    results.sort(key=lambda x: x["fraud_score"], reverse=True)
    return results


def run_antifraud(accounts, app_id, am_token, source_param, reg_event,
                  date_from, date_to, progress_cb=None):
    """Главная функция: запуск полного цикла антифрода."""
    if progress_cb:
        progress_cb("Загружаю данные из Директа...")

    direct_rows = fetch_direct_placements(accounts, date_from, date_to, progress_cb)

    if progress_cb:
        progress_cb("Загружаю установки из AppMetrica...")
    installations = fetch_installations(app_id, am_token, date_from, date_to, progress_cb)

    if progress_cb:
        progress_cb("Загружаю события из AppMetrica...")
    events = fetch_events(app_id, am_token, date_from, date_to, progress_cb)

    if progress_cb:
        progress_cb("Анализирую площадки...")
    placements, direct_names = build_placement_data(
        direct_rows, installations, events, source_param, reg_event
    )

    results = detect_fraud(placements, direct_names)

    stats = {
        "direct_rows": len(direct_rows),
        "installations": len(installations),
        "events": len(events),
        "direct_placements": len(direct_names),
        "analyzed": len(results),
        "critical": len([r for r in results if r["risk_level"] == "CRITICAL"]),
        "high": len([r for r in results if r["risk_level"] == "HIGH"]),
        "medium": len([r for r in results if r["risk_level"] == "MEDIUM"]),
        "low": len([r for r in results if r["risk_level"] == "LOW"]),
        "suspect_cost": sum(r["cost"] for r in results if r["risk_level"] in ("CRITICAL", "HIGH")),
    }

    return results, stats
