"""
БАЗАР Антифрод v1.4 — Streamlit UI
====================================
Веб-интерфейс для антифрод-системы.

Запуск локально: streamlit run app.py
Деплой: Streamlit Cloud (бесплатно)
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from antifraud_engine import run_antifraud

# ===================================================================
# КОНФИГУРАЦИЯ СТРАНИЦЫ
# ===================================================================
st.set_page_config(
    page_title="БАЗАР Антифрод",
    page_icon="🛡️",
    layout="wide",
)

# ===================================================================
# СТИЛИ
# ===================================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap');

    .main .block-container { padding-top: 2rem; max-width: 1200px; }

    .header-box {
        background: linear-gradient(135deg, #0f1923 0%, #1a2f44 100%);
        border-radius: 12px;
        padding: 2rem 2.5rem;
        margin-bottom: 1.5rem;
        border: 1px solid #2a4a6b;
    }
    .header-box h1 {
        color: #e8edf2;
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.8rem;
        margin: 0 0 0.3rem 0;
        letter-spacing: -0.5px;
    }
    .header-box p {
        color: #7a98b5;
        font-size: 0.95rem;
        margin: 0;
    }

    .metric-row { display: flex; gap: 12px; margin-bottom: 1.5rem; }
    .metric-card {
        flex: 1;
        border-radius: 10px;
        padding: 1rem 1.2rem;
        border: 1px solid #e0e0e0;
        background: #fafafa;
    }
    .metric-card .label { font-size: 0.75rem; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }
    .metric-card .value { font-size: 1.6rem; font-weight: 700; font-family: 'JetBrains Mono', monospace; margin-top: 2px; }

    .risk-critical { background: #fef2f2; border-color: #fca5a5; }
    .risk-critical .value { color: #991b1b; }
    .risk-high { background: #fffbeb; border-color: #fcd34d; }
    .risk-high .value { color: #854d0e; }
    .risk-medium { background: #eff6ff; border-color: #93c5fd; }
    .risk-medium .value { color: #1e40af; }
    .risk-low { background: #f0fdf4; border-color: #86efac; }
    .risk-low .value { color: #166534; }
    .risk-cost { background: #fefce8; border-color: #fde047; }
    .risk-cost .value { color: #713f12; }

    div[data-testid="stDataFrame"] { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)


# ===================================================================
# ЗАГРУЗКА СЕКРЕТОВ
# ===================================================================
def load_config():
    """Загружает конфиг из Streamlit Secrets или config.py."""
    try:
        # Streamlit Cloud: секреты из Settings → Secrets
        accounts = []
        i = 1
        while True:
            token_key = f"DIRECT_TOKEN_{i}"
            login_key = f"DIRECT_LOGIN_{i}"
            if token_key in st.secrets and login_key in st.secrets:
                accounts.append({"token": st.secrets[token_key], "login": st.secrets[login_key]})
                i += 1
            else:
                break

        if not accounts:
            # Попробуем старый формат
            if "DIRECT_TOKEN" in st.secrets:
                accounts = [{"token": st.secrets["DIRECT_TOKEN"], "login": st.secrets["DIRECT_LOGIN"]}]

        return {
            "accounts": accounts,
            "am_token": st.secrets["APPMETRICA_TOKEN"],
            "am_app_id": st.secrets["APPMETRICA_APP_ID"],
            "source_param": st.secrets.get("SOURCE_PARAM_NAME", "source"),
            "reg_event": st.secrets.get("REGISTRATION_EVENT_NAME", "registration_completed"),
        }
    except Exception:
        pass

    try:
        # Локальный запуск: config.py
        from config import (DIRECT_ACCOUNTS, APPMETRICA_TOKEN, APPMETRICA_APP_ID,
                            SOURCE_PARAM_NAME, REGISTRATION_EVENT_NAME)
        return {
            "accounts": DIRECT_ACCOUNTS,
            "am_token": APPMETRICA_TOKEN,
            "am_app_id": APPMETRICA_APP_ID,
            "source_param": SOURCE_PARAM_NAME,
            "reg_event": REGISTRATION_EVENT_NAME,
        }
    except Exception:
        return None


# ===================================================================
# СТИЛИЗАЦИЯ ТАБЛИЦЫ
# ===================================================================
def style_risk(val):
    colors = {
        "CRITICAL": "background-color: #fef2f2; color: #991b1b; font-weight: 700;",
        "HIGH": "background-color: #fffbeb; color: #854d0e; font-weight: 700;",
        "MEDIUM": "background-color: #eff6ff; color: #1e40af; font-weight: 600;",
        "LOW": "color: #6b7280;",
    }
    return colors.get(val, "")


# ===================================================================
# ОСНОВНОЙ ИНТЕРФЕЙС
# ===================================================================

# Заголовок
st.markdown("""
<div class="header-box">
    <h1>🛡️ БАЗАР Антифрод</h1>
    <p>Автоматический поиск фродовых площадок в РСЯ • 7 детекторов • v1.4</p>
</div>
""", unsafe_allow_html=True)

config = load_config()

if not config:
    st.error("⚠️ Не найден конфиг. Для Streamlit Cloud — заполните Secrets. Для локального запуска — создайте config.py.")
    st.stop()

if not config["accounts"]:
    st.error("⚠️ Не найдены аккаунты Директа в конфиге.")
    st.stop()

# Панель управления
col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    date_from = st.date_input("Дата начала", value=datetime.now() - timedelta(days=7))
with col2:
    date_to = st.date_input("Дата конца", value=datetime.now())
with col3:
    st.write("")
    st.write("")
    run_button = st.button("🚀 Запустить анализ", type="primary", use_container_width=True)

if run_button:
    progress_bar = st.progress(0)
    status_text = st.empty()

    step = [0]
    total_steps = 5

    def update_progress(msg):
        step[0] += 0.15
        if step[0] > 0.95:
            step[0] = 0.95
        progress_bar.progress(step[0])
        status_text.text(msg)

    date_from_str = date_from.strftime("%Y-%m-%d")
    date_to_str = date_to.strftime("%Y-%m-%d")

    try:
        results, stats = run_antifraud(
            accounts=config["accounts"],
            app_id=config["am_app_id"],
            am_token=config["am_token"],
            source_param=config["source_param"],
            reg_event=config["reg_event"],
            date_from=date_from_str,
            date_to=date_to_str,
            progress_cb=update_progress,
        )

        progress_bar.progress(1.0)
        status_text.text("Готово!")

        # Метрики
        st.markdown(f"""
        <div class="metric-row">
            <div class="metric-card risk-critical">
                <div class="label">Critical</div>
                <div class="value">{stats['critical']}</div>
            </div>
            <div class="metric-card risk-high">
                <div class="label">High</div>
                <div class="value">{stats['high']}</div>
            </div>
            <div class="metric-card risk-medium">
                <div class="label">Medium</div>
                <div class="value">{stats['medium']}</div>
            </div>
            <div class="metric-card risk-low">
                <div class="label">Low</div>
                <div class="value">{stats['low']}</div>
            </div>
           
        </div>
        """, unsafe_allow_html=True)

        # Информация о данных
        st.caption(
            f"Период: {date_from_str} — {date_to_str} · "
            f"Строк из Директа: {stats['direct_rows']:,} · "
            f"Установок: {stats['installations']:,} · "
            f"Событий: {stats['events']:,} · "
            f"Площадок проанализировано: {stats['analyzed']}"
        )

        if results:
            df = pd.DataFrame(results)

            # Фильтры
            st.markdown("---")
            fcol1, fcol2 = st.columns([1, 3])
            with fcol1:
                risk_filter = st.multiselect(
                    "Фильтр по риску",
                    ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                    default=["CRITICAL", "HIGH", "MEDIUM"]
                )
            with fcol2:
                search = st.text_input("Поиск по названию площадки", "")

            filtered = df[df["risk_level"].isin(risk_filter)]
            if search:
                filtered = filtered[filtered["placement"].str.contains(search, case=False, na=False)]

            # Основные колонки для отображения
            display_cols = [
                "placement", "risk_level", "fraud_score",
                "cost", "clicks", "installs", "registrations",
                "cr_install_to_reg", "cvr_click_to_install", "avg_bounce_rate",
                "ctit_median_sec", "top_device_model_pct",
            ]
            display_cols = [c for c in display_cols if c in filtered.columns]

            styled = filtered[display_cols].style.map(
    style_risk, subset=["risk_level"]
).format({
                "cost": "{:,.0f} ₽",
                "cr_install_to_reg": "{:.1%}",
                "cvr_click_to_install": "{:.2%}",
                "avg_bounce_rate": "{:.0f}%",
                "top_device_model_pct": "{:.0%}",
            }, na_rep="—")

            st.dataframe(styled, use_container_width=True, height=600)

            # Детали по флагам (развернуть)
            with st.expander("📋 Полная таблица с флагами детекторов"):
                st.dataframe(filtered, use_container_width=True, height=400)

            # Скачивание CSV
            csv_data = df.to_csv(index=False, sep=";").encode("utf-8-sig")
            st.download_button(
                "📥 Скачать полный отчёт (CSV)",
                csv_data,
                f"antifraud_report_{date_to_str.replace('-', '')}.csv",
                "text/csv",
            )
        else:
            st.warning("Нет данных для анализа за выбранный период.")

    except Exception as e:
        progress_bar.empty()
        status_text.empty()
        st.error(f"Ошибка: {str(e)}")
        st.exception(e)

else:
    st.info("👆 Выберите период и нажмите «Запустить анализ»")
    st.markdown("""
    **Детекторы:**
    1. **CR-аномалия** — слишком высокая конверсия install → reg
    2. **Равномерный залив** — подозрительно ровный объём установок по дням
    3. **Аномалии сессий** — множественные системные события в сессии
    4. **Высокий bounce** — bounce rate > 80% (усиленный при ≥95% + расход >3000₽)
    5. **CTIT** — подозрительное время от клика до установки
    6. **CVR** — аномальная конверсия клик → установка
    7. **Девайс-ферма** — концентрация одной модели устройства >60%
    """)
