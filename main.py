# -*- coding: utf-8 -*-
"""
BazukaBets - Sistema Unificado
===============================
Integra em um único arquivo:
  • Site com dashboard de apostas (Railway)
  • Bot Telegram VIP completo (Discloud → Railway)
  • Checkout VIP via site (/vip)
  • Persistência 100% em SQLite (sem payments.json — resolve o problema do Railway)

Variáveis de ambiente necessárias:
  BOT_TOKEN, MP_ACCESS_TOKEN, SECRET_KEY, ADMIN_USER, ADMIN_PASS,
  SITE_SECRET_KEY, SITE_PUBLIC_URL (URL pública do Railway),
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS (opcional — para email)
"""

import os, re, uuid, json, hmac, hashlib, logging, threading, time, tempfile, shutil, smtplib, requests
import sqlite3, pandas as pd

# -- Suporte a PostgreSQL (Railway) ---------------------------------------
# Se DATABASE_URL estiver definida usa PostgreSQL; senao usa SQLite.
_PG_URL = os.environ.get('DATABASE_URL', '').strip()
_USE_PG = _PG_URL.startswith('postgres')
if _USE_PG:
    import psycopg2, psycopg2.extras
    if _PG_URL.startswith('postgres://'):
        _PG_URL = 'postgresql://' + _PG_URL[len('postgres://'):]
import telebot
import mercadopago
import pytz

from flask import Flask, render_template, request, jsonify, session
from io import BytesIO
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import wraps
from collections import defaultdict
from email.message import EmailMessage
from urllib.parse import urlparse
from werkzeug.security import generate_password_hash, check_password_hash
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from typing import Optional, Dict, Any

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

BOT_TOKEN         = os.environ.get('BOT_TOKEN',        '8363865808:AAFM3x0a2aiO7ESwUaQK-H4w5fH0eYOE1UU')
MP_ACCESS_TOKEN   = os.environ.get('MP_ACCESS_TOKEN',  'APP_USR-6797918640127185-112319-1c452a696a8c3b443de9b0fe2baa9c01-318433737')
ADMIN_USER        = os.environ.get('ADMIN_USER',       'bazukabets@gmail.com')
ADMIN_PASS        = os.environ.get('ADMIN_PASS',       '102030Mm..')
SITE_SECRET_KEY   = os.environ.get('SITE_SECRET_KEY',  'uB5G6Zfn8Ij7wJonqYslBRx3vpBq5ZJq')
SITE_PUBLIC_URL   = os.environ.get('SITE_PUBLIC_URL',  '').strip().rstrip('/')  # Ex: https://seu-app.railway.app
SMTP_HOST         = os.environ.get('SMTP_HOST',        '').strip()
SMTP_PORT         = int(os.environ.get('SMTP_PORT',    '587'))
SMTP_USER         = os.environ.get('SMTP_USER',        '').strip()
SMTP_PASS         = os.environ.get('SMTP_PASS',        '')
SMTP_FROM         = os.environ.get('SMTP_FROM',        '') or SMTP_USER or ADMIN_USER
SMTP_USE_TLS      = os.environ.get('SMTP_USE_TLS',     'true').strip().lower() not in ('0', 'false', 'no')

TZ = pytz.timezone('America/Sao_Paulo')

MAX_VIP                = 30
LIMITE_PROMO           = 15
VALOR_PRIMEIRO_MES     = 250.0
VALOR_RENOVACAO_PROMO  = 350.0
VALOR_RENOVACAO_PADRAO = 450.0
VALOR_VIP              = 250.0
VALOR_VIP_RECORRENTE   = 350.0
DAYS_TESTE             = 3
DAYS_VIP               = 30
ID_GRUPO_VIP           = -1002915685276
ID_CANAL_LOGS          = -1003228897605
ADMIN_ID               = 8537674009

# URL do webhook do MP aponta para o próprio site (Railway)
NOTIFICATION_URL_BOT  = f"{SITE_PUBLIC_URL}/mercadopago_webhook" if SITE_PUBLIC_URL else ""
NOTIFICATION_URL_SITE = f"{SITE_PUBLIC_URL}/api/vip/webhook"     if SITE_PUBLIC_URL else ""

DB_PATH = os.environ.get('DB_PATH', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bazuka.db'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app    = Flask(__name__, template_folder='templates', static_folder='static')
app.secret_key = os.environ.get('SECRET_KEY', 'bazuka-secret-change-me')
bot    = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")
mp_sdk = mercadopago.SDK(MP_ACCESS_TOKEN)

# ══════════════════════════════════════════════════════════════════════════════
# BANCO DE DADOS (SQLite — persiste no Railway via volume ou DB_PATH)
# ══════════════════════════════════════════════════════════════════════════════

@contextmanager
def get_db():
    if _USE_PG:
        # PostgreSQL: wrapper que imita a API do sqlite3 para o código existente
        # nao precisar mudar nada.
        _raw = psycopg2.connect(_PG_URL)
        _raw.autocommit = False

        class _PGCursor:
            def __init__(self, cur):
                self._cur = cur
            def fetchone(self):
                row = self._cur.fetchone()
                return dict(row) if row else None
            def fetchall(self):
                return [dict(r) for r in (self._cur.fetchall() or [])]
            def __iter__(self):
                return iter(self.fetchall())
            def __getitem__(self, key):
                # permite acesso row[0] em fetchone
                row = self.fetchone()
                return row[key] if row else None

        class _PGConn:
            def __init__(self, raw):
                self._raw = raw
            def execute(self, sql, params=()):
                sql_pg = sql.replace('?', '%s')
                # SQLite-only INSERT OR REPLACE / OR IGNORE
                if 'INSERT OR IGNORE INTO bot_processed_payments' in sql:
                    sql_pg = sql_pg.replace(
                        'INSERT OR IGNORE INTO bot_processed_payments',
                        'INSERT INTO bot_processed_payments')
                    sql_pg = sql_pg.rstrip() + ' ON CONFLICT DO NOTHING'
                elif 'INSERT OR REPLACE INTO bot_config' in sql or \
                     'INSERT OR IGNORE INTO bot_config' in sql:
                    sql_pg = 'INSERT INTO bot_config (key,value) VALUES (%s,%s) '\
                             'ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value'
                elif 'INSERT OR REPLACE INTO vip_codes' in sql:
                    sql_pg = sql_pg.replace(
                        'INSERT OR REPLACE INTO vip_codes',
                        'INSERT INTO vip_codes')
                    sql_pg = sql_pg.rstrip() + ' ON CONFLICT (code) DO UPDATE SET '\
                             'telegram_id=EXCLUDED.telegram_id, '\
                             'email=EXCLUDED.email, '\
                             'payment_id=EXCLUDED.payment_id, '\
                             'created_at=EXCLUDED.created_at, '\
                             'used=0, used_at=NULL'
                cur = self._raw.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(sql_pg, params if params else None)
                return _PGCursor(cur)
            def commit(self):
                self._raw.commit()
            def close(self):
                self._raw.close()

        conn = _PGConn(_raw)
        try:
            yield conn
        except Exception:
            _raw.rollback()
            raise
        finally:
            _raw.close()
    else:
        # SQLite (desenvolvimento local ou fallback)
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()



def init_db():
    with get_db() as conn:
        # ── Tabelas do site ──────────────────────────────────────────────────
        conn.execute('''
            CREATE TABLE IF NOT EXISTS apostas (
                id TEXT PRIMARY KEY, liga TEXT, data TEXT, hora TEXT,
                confronto TEXT, mercado TEXT, odd REAL,
                resultado TEXT, lucro REAL, faixa_horaria TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY, nome TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                approved INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                active_until TEXT, paused_at TEXT, created_at TEXT
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS banca_apostas (
                id TEXT PRIMARY KEY, user_id TEXT NOT NULL,
                liga TEXT, data TEXT, hora TEXT, confronto TEXT,
                mercado TEXT, odd REAL, resultado TEXT,
                valor_apostado REAL, lucro REAL, created_at TEXT
            )
        ''')
        # ── Tabela de pagamentos VIP via site ───────────────────────────────
        conn.execute('''
            CREATE TABLE IF NOT EXISTS vip_payments (
                id TEXT PRIMARY KEY, telegram_id TEXT, email TEXT,
                mp_payment_id TEXT, mp_preference_id TEXT,
                status TEXT DEFAULT 'pending', vip_code TEXT,
                created_at TEXT, approved_at TEXT
            )
        ''')
        # ── Tabela principal do bot (substitui payments.json) ───────────────
        # Cada linha = um usuário do Telegram + seu status VIP
        conn.execute('''
            CREATE TABLE IF NOT EXISTS bot_payments (
                user_id TEXT PRIMARY KEY,
                status TEXT DEFAULT 'novo',
                start_date TEXT,
                end_date TEXT,
                invite_link_url TEXT,
                last_payment_id TEXT,
                teste_usado INTEGER DEFAULT 0,
                ja_pagou_vip INTEGER DEFAULT 0,
                renewal_notified INTEGER DEFAULT 0,
                last_pix_msg_id INTEGER,
                origem TEXT DEFAULT 'bot',
                created_at TEXT
            )
        ''')
        # processed_payments em tabela separada (relacionamento 1-N)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS bot_processed_payments (
                user_id TEXT NOT NULL,
                payment_id TEXT NOT NULL,
                processed_at TEXT,
                PRIMARY KEY (user_id, payment_id)
            )
        ''')
        # config do bot (ex: config_bot_parado)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS bot_config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        # vip_codes do site (substitui vip_codes.json)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS vip_codes (
                code TEXT PRIMARY KEY,
                telegram_id TEXT,
                email TEXT,
                payment_id TEXT,
                created_at TEXT,
                used INTEGER DEFAULT 0,
                used_at TEXT
            )
        ''')

        # Migracoes de colunas users
        if _USE_PG:
            for col, defn in [('status', "TEXT NOT NULL DEFAULT 'pending'"),
                              ('active_until', 'TEXT'), ('paused_at', 'TEXT')]:
                conn.execute(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {col} {defn}")
        else:
            existing_cols = {r['name'] for r in conn.execute("PRAGMA table_info(users)").fetchall()}
            for col, defn in [('status', "TEXT NOT NULL DEFAULT 'pending'"),
                              ('active_until', 'TEXT'), ('paused_at', 'TEXT')]:
                if col not in existing_cols:
                    conn.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")

        # Admin padrão
        conn.execute("UPDATE users SET status='active', active_until=NULL, paused_at=NULL WHERE role='admin'")
        conn.execute("UPDATE users SET status='pending'  WHERE role='user' AND approved=0 AND (status IS NULL OR status='')")
        conn.execute("UPDATE users SET status='active'   WHERE role='user' AND approved=1 AND (status IS NULL OR status='' OR status='pending')")

        admin_hash = generate_password_hash(ADMIN_PASS)
        admin = conn.execute('SELECT id FROM users WHERE email=?', (ADMIN_USER,)).fetchone()
        if admin:
            conn.execute('UPDATE users SET nome=?, password_hash=?, role=?, approved=1, status=?, active_until=NULL WHERE email=?',
                         ('Administrador Bazuka', admin_hash, 'admin', 'active', ADMIN_USER))
        else:
            conn.execute('''
                INSERT INTO users (id,nome,email,password_hash,role,approved,status,active_until,paused_at,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            ''', (uuid.uuid4().hex[:12], 'Administrador Bazuka', ADMIN_USER, admin_hash,
                  'admin', 1, 'active', None, None, datetime.utcnow().isoformat()))
        conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# CAMADA DE DADOS DO BOT  (substitui payments.json + vip_codes.json)
# ══════════════════════════════════════════════════════════════════════════════

def bot_get_user(user_id: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute('SELECT * FROM bot_payments WHERE user_id=?', (user_id,)).fetchone()
        if not row:
            return None
        d = dict(row)
        processed = [r['payment_id'] for r in
                     conn.execute('SELECT payment_id FROM bot_processed_payments WHERE user_id=?', (user_id,)).fetchall()]
        d['processed_payments'] = processed
        return d


def bot_upsert_user(user_id: str, data: dict):
    """Cria ou atualiza registro do usuário no bot_payments."""
    with get_db() as conn:
        existing = conn.execute('SELECT user_id FROM bot_payments WHERE user_id=?', (user_id,)).fetchone()
        if existing:
            sets, vals = [], []
            allowed = ['status','start_date','end_date','invite_link_url','last_payment_id',
                       'teste_usado','ja_pagou_vip','renewal_notified','last_pix_msg_id','origem']
            for k in allowed:
                if k in data:
                    sets.append(f"{k}=?")
                    val = data[k]
                    if isinstance(val, bool):
                        val = 1 if val else 0
                    vals.append(val)
            if sets:
                vals.append(user_id)
                conn.execute(f"UPDATE bot_payments SET {', '.join(sets)} WHERE user_id=?", vals)
        else:
            conn.execute('''
                INSERT INTO bot_payments (user_id,status,start_date,end_date,invite_link_url,
                    last_payment_id,teste_usado,ja_pagou_vip,renewal_notified,last_pix_msg_id,origem,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (
                user_id,
                data.get('status', 'novo'),
                data.get('start_date'),
                data.get('end_date'),
                data.get('invite_link_url'),
                data.get('last_payment_id'),
                1 if data.get('teste_usado') else 0,
                1 if data.get('ja_pagou_vip') else 0,
                1 if data.get('renewal_notified') else 0,
                data.get('last_pix_msg_id'),
                data.get('origem', 'bot'),
                datetime.utcnow().isoformat(),
            ))
        # processed_payments
        for pid in data.get('processed_payments', []):
            try:
                conn.execute('INSERT OR IGNORE INTO bot_processed_payments (user_id,payment_id,processed_at) VALUES (?,?,?)',
                             (user_id, pid, datetime.utcnow().isoformat()))
            except Exception:
                pass
        conn.commit()


def bot_add_processed_payment(user_id: str, payment_id: str):
    with get_db() as conn:
        conn.execute('INSERT OR IGNORE INTO bot_processed_payments (user_id,payment_id,processed_at) VALUES (?,?,?)',
                     (user_id, payment_id, datetime.utcnow().isoformat()))
        conn.commit()


def bot_is_payment_processed(user_id: str, payment_id: str) -> bool:
    with get_db() as conn:
        row = conn.execute('SELECT 1 FROM bot_processed_payments WHERE user_id=? AND payment_id=?',
                           (user_id, payment_id)).fetchone()
        return row is not None


def bot_iter_users():
    """Itera sobre todos os usuários (ignora chaves de config)."""
    with get_db() as conn:
        rows = conn.execute('SELECT * FROM bot_payments').fetchall()
    for row in rows:
        d = dict(row)
        d['teste_usado']     = bool(d.get('teste_usado'))
        d['ja_pagou_vip']    = bool(d.get('ja_pagou_vip'))
        d['renewal_notified']= bool(d.get('renewal_notified'))
        yield d['user_id'], d


def bot_get_config(key: str, default=None):
    with get_db() as conn:
        row = conn.execute('SELECT value FROM bot_config WHERE key=?', (key,)).fetchone()
        return row['value'] if row else default


def bot_set_config(key: str, value):
    with get_db() as conn:
        conn.execute('INSERT OR REPLACE INTO bot_config (key,value) VALUES (?,?)', (key, str(value)))
        conn.commit()


# ── VIP codes (substitui vip_codes.json) ────────────────────────────────────

def vip_code_save(code: str, telegram_id, email, payment_id: str):
    with get_db() as conn:
        conn.execute('''
            INSERT OR REPLACE INTO vip_codes (code,telegram_id,email,payment_id,created_at,used,used_at)
            VALUES (?,?,?,?,?,0,NULL)
        ''', (code, telegram_id or None, email or None, payment_id, datetime.utcnow().isoformat()))
        conn.commit()


def vip_code_get(code: str) -> Optional[dict]:
    with get_db() as conn:
        row = conn.execute('SELECT * FROM vip_codes WHERE code=?', (code,)).fetchone()
        return dict(row) if row else None


def vip_code_mark_used(code: str):
    with get_db() as conn:
        conn.execute("UPDATE vip_codes SET used=1, used_at=? WHERE code=?",
                     (datetime.utcnow().isoformat(), code))
        conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
# UTILITÁRIOS DO BOT
# ══════════════════════════════════════════════════════════════════════════════

def get_end_date_aware(end_date_str: str) -> Optional[datetime]:
    try:
        end = datetime.fromisoformat(end_date_str)
        if end.tzinfo is None:
            return TZ.localize(end)
        return end.astimezone(TZ)
    except Exception:
        return None


def is_member_in_group(user_id: str) -> bool:
    try:
        member = bot.get_chat_member(ID_GRUPO_VIP, int(user_id))
        return member.status in ("member", "administrator", "creator", "restricted")
    except Exception:
        return False


def log_to_channel(user_id, username, event, status=None, end_date=None):
    try:
        if not username or str(username) == "None":
            try:
                chat = bot.get_chat(int(user_id))
                username = chat.username if chat.username else f"ID_{user_id}"
            except Exception:
                username = f"ID_{user_id}"

        user_mention = (f"[@{username}](tg://user?id={user_id})"
                        if username and not str(username).startswith("ID_")
                        else f"ID: `{user_id}`")

        event_map = {
            "ENTROU_TESTE":       "Entrou via TESTE GRATUITO",
            "ENTROU_VIP":         "Entrou via VIP (Pago)",
            "ENTROU_VIP_SITE":    "Entrou via VIP (Site)",
            "REEMBOLSO_APROVADO": "Removido por REEMBOLSO",
            "RENOVOU_VIP":        "RENOVACAO Confirmada",
            "SAIU_TESTE":         "Removido (Fim do Teste)",
            "SAIU_VIP":           "Removido (Fim do VIP)",
        }
        parts = [
            f"*👤 Ação:* {user_mention}",
            f"*🔗 ID:* `{user_id}`",
            f"*⭐ EVENTO:* {event_map.get(event, event)}",
        ]
        if status:
            parts.append(f"*🏷️ Status:* `{str(status).upper()}`")
        if end_date:
            parts.append(f"*🗓️ Data:* {end_date}")
        bot.send_message(ID_CANAL_LOGS, "\n".join(parts), parse_mode="Markdown")
    except Exception as e:
        logging.error(f"FALHA AO ENVIAR LOG: {e}")


def create_invite_link(user_id, days):
    try:
        expire_date = int(time.time()) + days * 24 * 3600
        return bot.create_chat_invite_link(
            chat_id=ID_GRUPO_VIP,
            name=f"Acesso-{user_id}",
            expire_date=expire_date,
            creates_join_request=True,
        )
    except Exception as e:
        logging.error(f"Erro ao criar link para {user_id}: {e}")
        return None


def get_user_vip_rank(user_id):
    vips_ativos = []
    now = datetime.now(TZ)
    for uid, info in bot_iter_users():
        if info.get("status") == "vip":
            end_str = info.get("end_date")
            if end_str:
                end_aware = get_end_date_aware(end_str)
                if end_aware and end_aware > now:
                    vips_ativos.append((info.get("start_date"), uid))
    vips_ativos.sort()
    total_ativos = len(vips_ativos)
    user_position = total_ativos + 1
    for i, (_, uid) in enumerate(vips_ativos):
        if uid == user_id:
            user_position = i + 1
            break
    valor = VALOR_RENOVACAO_PROMO if user_position <= LIMITE_PROMO else VALOR_RENOVACAO_PADRAO
    return user_position, valor, total_ativos


def avisar_vaga_liberada():
    for uid, info in bot_iter_users():
        if info.get("status") == "waiting":
            try:
                bot.send_message(int(uid),
                    "*VAGA LIBERADA NO VIP!*\n\nAlguem acaba de sair e uma vaga abriu. "
                    "Use /start para assinar.")
                time.sleep(0.1)
            except Exception:
                continue


def perform_kick(user_id: str, reason: str):
    try:
        bot.ban_chat_member(ID_GRUPO_VIP, int(user_id), until_date=int(time.time()) + 1)
        try:
            bot.unban_chat_member(ID_GRUPO_VIP, int(user_id))
        except Exception:
            pass

        user_data = bot_get_user(user_id)
        if user_data:
            current_status = user_data.get("status")
            if current_status in ("vip", "teste"):
                bot_upsert_user(user_id, {"status": "expired", "renewal_notified": False})
        try:
            bot.send_message(int(user_id),
                f"Seu acesso ao grupo VIP ({reason.upper()}) expirou.\nPara renovar, use /start.")
        except Exception:
            pass
        avisar_vaga_liberada()
    except telebot.apihelper.ApiTelegramException as e:
        if 'USER_NOT_PARTICIPANT' in str(e) or 'user not found' in str(e).lower():
            user_data = bot_get_user(user_id)
            if user_data and user_data.get("status") in ("vip", "teste"):
                bot_upsert_user(user_id, {"status": "expired", "renewal_notified": False})
        else:
            logging.error(f"Erro ao remover {user_id}: {e}")
    except Exception as e:
        logging.error(f"Erro em perform_kick para {user_id}: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# HANDLERS DO BOT TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════

@bot.message_handler(commands=['stop'])
def stop_bot(message):
    if message.from_user.id != ADMIN_ID:
        return
    bot_set_config('bot_parado', 'true')
    bot.reply_to(message, "*SISTEMA PAUSADO*\n\nNovas entradas e menu de vendas desativados.")


@bot.message_handler(commands=['voltar'])
def voltar_bot(message):
    if message.from_user.id != ADMIN_ID:
        return
    bot_set_config('bot_parado', 'false')
    bot.reply_to(message, "*SISTEMA REATIVADO*\n\nComandos voltaram a funcionar normalmente.")


@bot.message_handler(commands=['start'])
def start_menu(message):
    try:
        user_id = str(message.from_user.id)
        user_data = bot_get_user(user_id)
        if not user_data:
            bot_upsert_user(user_id, {"status": "novo", "teste_usado": False})
            user_data = bot_get_user(user_id)

        keyboard = InlineKeyboardMarkup()
        if not user_data.get("teste_usado"):
            keyboard.add(InlineKeyboardButton("💎 TESTE GRATUITO", callback_data="teste_gratis"))
        keyboard.add(InlineKeyboardButton("🏆 ADQUIRIR VIP", callback_data="adquirir_vip"))
        keyboard.add(InlineKeyboardButton("💻 COMPRAR PELO SITE", url=f"{SITE_PUBLIC_URL}/vip" if SITE_PUBLIC_URL else "https://bazukabets.com/vip"))

        bot.send_message(message.chat.id,
            f"👋 Olá, {message.from_user.first_name}!\n\nEscolha uma opção:",
            reply_markup=keyboard)
    except Exception as e:
        logging.error(f"Erro em start_menu: {e}")
        bot.send_message(message.chat.id, "Erro interno. Tente novamente.")


@bot.message_handler(commands=['apostabrasil'])
def special_menu(message):
    user_id = str(message.from_user.id)
    if bot_get_config('bot_parado') == 'true':
        bot.send_message(message.chat.id, "*BONUS INDISPONIVEL*\n\nAdesoes temporariamente pausadas.")
        return
    user_data = bot_get_user(user_id) or {}
    keyboard = InlineKeyboardMarkup()
    if not user_data.get("teste_usado"):
        keyboard.add(InlineKeyboardButton("TESTE ESPECIAL (4 dias)", callback_data="teste_4dias"))
    else:
        bot.send_message(message.chat.id, "Voce ja utilizou seu periodo de teste.")
        return
    keyboard.add(InlineKeyboardButton("ADQUIRIR VIP", callback_data="adquirir_vip"))
    bot.send_message(message.chat.id,
        "*Bonus Aposta Brasil Ativado!*\n\nVoce ganhou 4 dias de teste gratuito.",
        reply_markup=keyboard)


@bot.message_handler(commands=['apostafacil'])
def apostafacil_handler(message):
    user_id = str(message.from_user.id)
    if not bot_get_user(user_id):
        bot_upsert_user(user_id, {"status": "novo", "teste_usado": False})
    start_menu(message)


@bot.callback_query_handler(func=lambda call: call.data in ["teste_gratis", "teste_4dias"])
def handle_teste_gratis(call):
    user_id  = str(call.from_user.id)
    username = call.from_user.username
    now      = datetime.now(TZ)

    dias_concedidos = 4 if call.data == "teste_4dias" else DAYS_TESTE
    label_log       = "teste_4dias_bonus" if call.data == "teste_4dias" else "teste_3dias_padrao"

    user_data = bot_get_user(user_id) or {}

    if user_data.get("status") in ["vip", "teste"]:
        end_str = user_data.get("end_date")
        if end_str:
            end_aware = get_end_date_aware(end_str)
            if end_aware and end_aware > now:
                bot.answer_callback_query(call.id,
                    f"Voce ja tem acesso ativo ate {end_aware.strftime('%d/%m %H:%M')}.",
                    show_alert=True)
                return

    if user_data.get("teste_usado"):
        bot.answer_callback_query(call.id, "Voce ja usou seu teste gratuito.")
        adquirir_vip(call)
        return

    try:
        bot.edit_message_text(chat_id=call.message.chat.id,
                              message_id=call.message.message_id,
                              text="✅ Processando seu acesso de teste...")
    except Exception:
        pass

    try:
        bot.unban_chat_member(ID_GRUPO_VIP, int(user_id))
    except Exception:
        pass

    link_obj = create_invite_link(user_id, days=dias_concedidos)
    if not link_obj:
        bot.send_message(int(user_id), "Erro ao gerar link. Contate o suporte.")
        return

    link  = link_obj.invite_link
    start = datetime.now(TZ)
    end   = start + timedelta(days=dias_concedidos)

    bot_upsert_user(user_id, {
        "status":          "teste",
        "start_date":      start.isoformat(),
        "end_date":        end.isoformat(),
        "teste_usado":     True,
        "invite_link_url": link,
    })

    try:
        bot.send_message(int(user_id),
            f"*🎉 TESTE LIBERADO!*\n"
            f"⏳ Duracao: *{dias_concedidos} dias*\n"
            f"📅 Expira em: {end.strftime('%d/%m/%Y as %H:%M')}\n\n"
            f"👉 *Clique no link para entrar no grupo:*\n{link}")
        bot.answer_callback_query(call.id)
    except Exception as e:
        logging.error(f"Erro ao enviar link de teste: {e}")

    try:
        log_to_channel(user_id, username, "ENTROU_TESTE", status=label_log,
                       end_date=end.strftime("%d/%m/%Y %H:%M"))
    except Exception:
        pass


@bot.callback_query_handler(func=lambda call: call.data == "adquirir_vip")
def adquirir_vip(call):
    user_id   = str(call.from_user.id)
    user_data = bot_get_user(user_id) or {}

    pos, valor_renovacao, total_vips = get_user_vip_rank(user_id)

    if total_vips >= MAX_VIP and user_data.get("status") != "vip":
        bot.answer_callback_query(call.id,
            "O Grupo VIP atingiu o limite de 30 membros. Aguarde uma vaga.",
            show_alert=True)
        return

    ja_foi_vip  = user_data.get("ja_pagou_vip", False)
    valor_final = VALOR_PRIMEIRO_MES if not ja_foi_vip else valor_renovacao
    texto_valor = "Primeiro Mes" if not ja_foi_vip else "Renovacao"

    if call.message:
        try:
            bot.edit_message_text(
                chat_id=call.message.chat.id,
                message_id=call.message.message_id,
                text=f"✅ Opção VIP selecionada.\n\nGerando PIX de R$ {valor_final:.2f}...")
        except Exception:
            pass

    payment_data = {
        "transaction_amount": float(valor_final),
        "description":        f"Assinatura VIP BazukaBets - {texto_valor}",
        "payment_method_id":  "pix",
        "payer": {"email": f"user_{user_id}@bazukabets.com", "first_name": "Cliente", "last_name": user_id},
        "external_reference": str(user_id),
    }
    if NOTIFICATION_URL_BOT:
        payment_data["notification_url"] = NOTIFICATION_URL_BOT

    try:
        response = mp_sdk.payment().create(payment_data)
        if response.get("status") != 201:
            raise Exception(f"MP retornou {response.get('status')}")

        qr_info  = response["response"]["point_of_interaction"]["transaction_data"]
        pix_code = qr_info.get("qr_code")

        bot.answer_callback_query(call.id)

        chat_id_destino = call.message.chat.id if call.message else int(user_id)
        msg_pix = bot.send_message(chat_id_destino,
            f"*💰 PIX PARA PAGAMENTO: R$ {valor_final:.2f}*\n\n"
            f"`{pix_code}`\n\n"
            f"Copie o codigo acima e pague no seu banco.\n"
            f"✅ Acesso liberado *automaticamente* apos confirmacao.")

        bot_upsert_user(user_id, {"last_pix_msg_id": msg_pix.message_id})

    except Exception as e:
        logging.error(f"Erro ao gerar PIX para {user_id}: {e}")
        chat_id_destino = call.message.chat.id if call.message else int(user_id)
        bot.send_message(chat_id_destino, "❌ Erro ao gerar PIX. Tente novamente ou contate o suporte.")


# ── Handler de código VIP do site (BBV-XXXXXXXX) ─────────────────────────────

VIP_CODE_PATTERN = re.compile(r'^BBV-[A-Z0-9]{8}$')


def normalize_vip_code(raw_text: str) -> str:
    text = (raw_text or "").strip().upper()
    for dash in ("\u2010", "\u2011", "\u2012", "\u2013", "\u2014", "\u2212"):
        text = text.replace(dash, "-")
    text = re.sub(r'\s+', '', text)
    if text.startswith("BBV") and not text.startswith("BBV-") and len(text) >= 4:
        text = "BBV-" + text[3:]
    return text


@bot.message_handler(func=lambda m: bool(m.text and VIP_CODE_PATTERN.match(normalize_vip_code(m.text))))
def handle_vip_code(message):
    user_id = str(message.from_user.id)
    code    = normalize_vip_code(message.text)

    code_data = vip_code_get(code)

    if not code_data:
        bot.send_message(message.chat.id,
            "❌ *Codigo invalido ou nao encontrado.*\n\n"
            "Verifique se copiou corretamente. Se o problema persistir, contate o suporte.")
        return

    if code_data.get("used"):
        bot.send_message(message.chat.id,
            "⚠️ *Este codigo ja foi utilizado.*\n\n"
            "Se voce ainda nao recebeu seu acesso, contate o suporte.")
        return

    if code_data.get("telegram_id") and code_data["telegram_id"] != user_id:
        bot.send_message(message.chat.id,
            "🚫 *Codigo nao pertence a esta conta.*\n\n"
            "Este codigo foi gerado para outro usuario do Telegram.")
        return

    payment_id = code_data.get("payment_id")
    if payment_id and bot_is_payment_processed(user_id, payment_id):
        bot.send_message(message.chat.id,
            "⚠️ *Pagamento ja processado.*\n\nSeu acesso VIP ja foi liberado anteriormente.")
        return

    now             = datetime.now(TZ)
    user_data       = bot_get_user(user_id) or {}
    previous_status = user_data.get("status")
    old_end_str     = user_data.get("end_date")
    old_end_aware   = get_end_date_aware(old_end_str) if old_end_str else None
    base_date       = old_end_aware if (old_end_aware and old_end_aware > now) else now
    new_end         = base_date + timedelta(days=DAYS_VIP)
    is_renewal      = (previous_status == "vip" and old_end_aware and old_end_aware > now)

    member_already_in_group = is_renewal and is_member_in_group(user_id)

    link = None
    if not member_already_in_group:
        try:
            bot.unban_chat_member(ID_GRUPO_VIP, int(user_id))
        except Exception:
            pass
        link_obj = create_invite_link(user_id, days=DAYS_VIP)
        link = link_obj.invite_link if link_obj else None
        if not link:
            bot.send_message(message.chat.id,
                "⚠️ Pagamento reconhecido, mas houve erro ao gerar o link. Contate o suporte.")
            return

    bot_upsert_user(user_id, {
        "status":           "vip",
        "ja_pagou_vip":     True,
        "start_date":       base_date.isoformat(),
        "end_date":         new_end.isoformat(),
        "invite_link_url":  link if link else user_data.get("invite_link_url"),
        "last_payment_id":  payment_id,
        "renewal_notified": False,
        "teste_usado":      user_data.get("teste_usado", True),
        "origem":           "site",
        "processed_payments": [payment_id] if payment_id else [],
    })

    vip_code_mark_used(code)

    if is_renewal and member_already_in_group:
        bot.send_message(message.chat.id,
            f"*✅ RENOVACAO CONFIRMADA!*\n\n"
            f"🎉 Voce *permanece no grupo* normalmente.\n"
            f"📅 +{DAYS_VIP} dias adicionados.\n"
            f"🗓️ Novo vencimento: *{new_end.strftime('%d/%m/%Y as %H:%M')}*")
    else:
        bot.send_message(message.chat.id,
            f"*✅ ACESSO VIP LIBERADO!*\n\n"
            f"🎉 Bem-vindo ao VIP BazukaBets!\n"
            f"🗓️ Seu acesso expira em: *{new_end.strftime('%d/%m/%Y as %H:%M')}*\n\n"
            f"👉 *Clique no link para entrar no grupo:*\n{link}")

    try:
        log_event = "RENOVOU_VIP" if is_renewal else "ENTROU_VIP_SITE"
        log_to_channel(user_id, message.from_user.username, log_event,
                       status="vip", end_date=new_end.strftime("%d/%m/%Y %H:%M"))
    except Exception:
        pass


@bot.message_handler(func=lambda m: bool(m.text and normalize_vip_code(m.text).startswith("BBV")))
def handle_invalid_vip_code(message):
    normalized = normalize_vip_code(message.text)
    bot.send_message(message.chat.id,
        f"❌ *Codigo VIP invalido.*\n\nRecebi: `{normalized}`\n\nO formato correto e `BBV-XXXXXXXX`.")


# ── Verificação periódica ─────────────────────────────────────────────────────

def periodic_check():
    while True:
        try:
            if bot_get_config('bot_parado') == 'true':
                time.sleep(60)
                continue

            now = datetime.now(TZ)
            logging.info(f"--- [CHECK] {now.strftime('%d/%m %H:%M')} ---")

            for user_id, info in list(bot_iter_users()):
                status       = info.get("status")
                end_date_str = info.get("end_date")
                if status not in ["vip", "teste"] or not end_date_str:
                    continue
                end = get_end_date_aware(end_date_str)
                if not end:
                    continue
                tempo_restante = end - now

                if status == "vip" and not info.get("renewal_notified"):
                    if timedelta(days=0) < tempo_restante <= timedelta(days=3):
                        keyboard = InlineKeyboardMarkup()
                        keyboard.add(InlineKeyboardButton("🔄 RENOVAR AGORA", callback_data=f"renew_{user_id}"))
                        try:
                            bot.send_message(int(user_id),
                                f"*⚠️ SEU VIP ESTA ACABANDO!*\n\n"
                                f"🗓️ Expira em: `{end.strftime('%d/%m/%Y as %H:%M')}`\n"
                                f"Renove agora para nao perder sua vaga!",
                                reply_markup=keyboard)
                            bot_upsert_user(user_id, {"renewal_notified": True})
                        except Exception:
                            pass

                if now > end:
                    logging.warning(f"EXPIRADO: Removendo {user_id}")
                    log_event = "SAIU_VIP" if status == "vip" else "SAIU_TESTE"
                    perform_kick(user_id, status)
                    try:
                        uname = bot.get_chat(int(user_id)).username
                    except Exception:
                        uname = None
                    log_to_channel(user_id, uname, log_event, status="EXPIRADO")
                    time.sleep(0.5)

        except Exception as e:
            logging.error(f"Erro no periodic_check: {e}")

        time.sleep(60)


# ── Renovação via bot ─────────────────────────────────────────────────────────

@bot.callback_query_handler(func=lambda call: call.data.startswith("renew_"))
def handle_renew(call):
    if bot_get_config('bot_parado') == 'true':
        bot.answer_callback_query(call.id, "Renovacoes suspensas pelo administrador.", show_alert=True)
        return
    adquirir_vip(call)


# ── Porteiro ──────────────────────────────────────────────────────────────────

@bot.chat_join_request_handler()
def validar_entrada_por_link(update):
    user_id_clicou    = str(update.from_user.id)
    chat_id           = update.chat.id
    invite_link_usado = update.invite_link.invite_link if update.invite_link else None

    if chat_id != ID_GRUPO_VIP:
        return

    if not invite_link_usado:
        try:
            bot.decline_chat_join_request(chat_id, int(user_id_clicou))
        except Exception:
            pass
        return

    dono_do_link = None
    dados_venda  = None
    for uid, info in bot_iter_users():
        if info.get("invite_link_url") == invite_link_usado:
            dono_do_link = uid
            dados_venda  = info
            break

    if not dono_do_link:
        try:
            bot.decline_chat_join_request(chat_id, int(user_id_clicou))
        except Exception:
            pass
        return

    if user_id_clicou != dono_do_link:
        try:
            bot.send_message(int(user_id_clicou),
                "*🚫 Acesso Negado*\n\nEste link é pessoal e só funciona para a conta que realizou o pagamento.")
            bot.decline_chat_join_request(chat_id, int(user_id_clicou))
        except Exception:
            pass
        return

    status   = dados_venda.get("status")
    end_str  = dados_venda.get("end_date")
    end_date = get_end_date_aware(end_str) if end_str else None

    if status in ["vip", "teste"] and end_date and end_date > datetime.now(TZ):
        try:
            bot.approve_chat_join_request(chat_id, int(user_id_clicou))
        except Exception as e:
            logging.error(f"Erro ao aprovar {user_id_clicou}: {e}")
    else:
        try:
            bot.decline_chat_join_request(chat_id, int(user_id_clicou))
            bot.send_message(int(user_id_clicou),
                "⏰ *Link expirado ou acesso encerrado.*\n\nRenove seu acesso VIP via /start.")
        except Exception:
            pass


# ── Reembolso ─────────────────────────────────────────────────────────────────

@bot.message_handler(commands=['reembolso'])
def solicitar_reembolso(message):
    user_id   = str(message.from_user.id)
    user_data = bot_get_user(user_id) or {}
    if user_data.get("status") != "vip":
        bot.reply_to(message, "Apenas membros VIP ativos podem solicitar reembolso.")
        return
    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton("✅ ACEITAR E DEVOLVER PIX", callback_data=f"ref_ok_{user_id}"),
        InlineKeyboardButton("❌ RECUSAR",                callback_data=f"ref_no_{user_id}"),
    )
    bot.send_message(ADMIN_ID,
        f"*💰 PEDIDO DE REEMBOLSO*\n\n"
        f"Usuario: @{message.from_user.username or 'Sem Username'}\n"
        f"ID: `{user_id}`\n"
        f"ID Pagamento: `{user_data.get('last_payment_id', 'N/A')}`\n\n"
        f"Deseja devolver o dinheiro e remover o membro?",
        reply_markup=markup)
    bot.reply_to(message, "✅ Sua solicitacao foi enviada ao administrador.")


@bot.callback_query_handler(func=lambda call: call.data.startswith("ref_"))
def processar_reembolso_admin(call):
    partes    = call.data.split("_", 2)
    acao      = partes[1]
    target_id = partes[2]

    if acao == "no":
        bot.edit_message_text("Reembolso recusado.", call.message.chat.id, call.message.message_id)
        try:
            bot.send_message(int(target_id), "Seu pedido de reembolso foi recusado.")
        except Exception:
            pass
        return

    user_data = bot_get_user(target_id) or {}
    p_id = user_data.get("last_payment_id")
    if not p_id:
        bot.answer_callback_query(call.id, "Erro: ID de pagamento nao encontrado.", show_alert=True)
        return

    try:
        refund_res = mp_sdk.refund().create(str(p_id))
        if refund_res.get("status") in [200, 201]:
            try:
                u_name = bot.get_chat(int(target_id)).username
            except Exception:
                u_name = None
            bot_upsert_user(target_id, {"status": "reembolsado", "end_date": datetime.now(TZ).isoformat()})
            log_to_channel(target_id, u_name, "REEMBOLSO_APROVADO", status="reembolsado")
            try:
                bot.ban_chat_member(ID_GRUPO_VIP, int(target_id), until_date=int(time.time()) + 1)
                bot.unban_chat_member(ID_GRUPO_VIP, int(target_id))
            except Exception as e:
                logging.error(f"Erro ao remover {target_id} no reembolso: {e}")
            bot.edit_message_text(f"✅ Reembolso do ID `{target_id}` processado!",
                                  call.message.chat.id, call.message.message_id)
            try:
                bot.send_message(int(target_id), "✅ Seu reembolso foi aprovado! Acesso VIP encerrado.")
            except Exception:
                pass
        else:
            erro = refund_res.get("response", {}).get("message", "Erro desconhecido")
            bot.send_message(ADMIN_ID, f"❌ Erro na API MP: {erro}")
    except Exception as e:
        bot.send_message(ADMIN_ID, f"❌ Erro interno ao processar estorno: {e}")


# ── Comunicados ───────────────────────────────────────────────────────────────

@bot.message_handler(commands=['vips'])
def iniciar_comunicado_vip(message):
    if message.from_user.id != ADMIN_ID:
        return
    msg = bot.send_message(message.chat.id,
        "*📢 MODO COMUNICADO VIP*\n\nEnvie a mensagem para os membros VIP.\n\n/cancelar para cancelar.")
    bot.register_next_step_handler(msg, disparar_comunicado_vip)


def disparar_comunicado_vip(message):
    if message.text == '/cancelar':
        bot.send_message(message.chat.id, "Envio cancelado.")
        return
    lista_vips = [uid for uid, info in bot_iter_users() if info.get("status") == "vip"]
    if not lista_vips:
        bot.send_message(message.chat.id, "Nenhum VIP ativo no momento.")
        return
    sucesso = falha = 0
    for uid in lista_vips:
        try:
            bot.send_message(int(uid), message.text, parse_mode="Markdown")
            sucesso += 1
            time.sleep(0.05)
        except Exception:
            falha += 1
    bot.send_message(message.chat.id, f"*Disparo VIP concluido!*\n\n✅ Sucesso: {sucesso}\n❌ Falhas: {falha}")


@bot.message_handler(commands=['comunicado'])
def iniciar_comunicado(message):
    if message.from_user.id != ADMIN_ID:
        return
    msg = bot.send_message(message.chat.id, "*📢 Modo Transmissao*\n\nEnvie a mensagem para TODOS os usuarios.")
    bot.register_next_step_handler(msg, disparar_comunicado)


def disparar_comunicado(message):
    if message.text == '/cancelar':
        bot.send_message(message.chat.id, "Envio cancelado.")
        return
    lista = [uid for uid, _ in bot_iter_users()]
    sucesso = falha = 0
    for uid in lista:
        try:
            bot.send_message(int(uid), message.text, parse_mode="Markdown")
            sucesso += 1
            time.sleep(0.05)
        except Exception:
            falha += 1
    bot.send_message(message.chat.id, f"*Disparo concluido!*\n\n✅ Sucesso: {sucesso}\n❌ Falhas: {falha}")


@bot.message_handler(commands=['imagem'])
def iniciar_transmissao_foto(message):
    if message.from_user.id != ADMIN_ID:
        return
    msg = bot.send_message(message.chat.id,
        "*🖼️ Modo Transmissao de Imagem*\n\nEnvie agora a foto (com ou sem legenda).")
    bot.register_next_step_handler(msg, disparar_foto)


def disparar_foto(message):
    if not message.photo:
        bot.send_message(message.chat.id, "Voce nao enviou uma imagem.")
        return
    photo_id = message.photo[-1].file_id
    legenda  = message.caption
    lista    = [uid for uid, _ in bot_iter_users()]
    sucesso = falha = 0
    for uid in lista:
        try:
            bot.send_photo(int(uid), photo_id, caption=legenda, parse_mode="Markdown")
            sucesso += 1
            time.sleep(0.08)
        except Exception:
            falha += 1
    bot.send_message(message.chat.id, f"*Disparo de imagem concluido!*\n\n✅ Sucesso: {sucesso}\n❌ Falhas: {falha}")


# ══════════════════════════════════════════════════════════════════════════════
# ROTAS FLASK — WEBHOOKS DO TELEGRAM E MERCADO PAGO
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/telegram_webhook", methods=["POST"])
def telegram_webhook():
    update = telebot.types.Update.de_json(request.json)
    bot.process_new_updates([update])
    return "OK", 200


@app.route("/mercadopago_webhook", methods=["POST"])
def mercadopago_webhook():
    """Webhook do Mercado Pago para pagamentos feitos via BOT (external_reference = telegram_id)."""
    try:
        data = request.json or {}
        payment_id = None

        resource_url = data.get("resource")
        if resource_url and "/payments/" in str(resource_url):
            raw_id     = str(resource_url).split("/payments/")[-1].strip()
            payment_id = "".join(filter(str.isdigit, raw_id))
        if not payment_id and isinstance(data.get("data"), dict):
            payment_id = str(data["data"].get("id", ""))

        if not payment_id:
            return jsonify({"status": "no_id"}), 400

        url  = f"https://api.mercadopago.com/v1/payments/{payment_id}"
        resp = requests.get(url, headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}"}, timeout=10)
        if not resp.ok:
            return jsonify({"status": "mp_fetch_error"}), 500

        info_mp        = resp.json()
        current_status = info_mp.get("status")
        ext_ref        = str(info_mp.get("external_reference", "")).strip()
        user_id        = ext_ref if ext_ref and not ext_ref.startswith("SITEVIP|") else None

        if not user_id:
            # Pagamento do site — delega para o handler do site
            if ext_ref.startswith("SITEVIP|"):
                return _finalize_site_vip_payment(payment_id, info_mp)
            return jsonify({"status": "no_user"}), 200

        if current_status != "approved":
            return jsonify({"status": "waiting"}), 200

        if bot_is_payment_processed(user_id, payment_id):
            return jsonify({"status": "already_processed"}), 200

        now             = datetime.now(TZ)
        user_data       = bot_get_user(user_id) or {}
        username        = None
        try:
            username = bot.get_chat(int(user_id)).username
        except Exception:
            pass

        previous_status = user_data.get("status")
        old_end_str     = user_data.get("end_date")
        old_end_aware   = get_end_date_aware(old_end_str) if old_end_str else None
        base_date       = old_end_aware if (old_end_aware and old_end_aware > now) else now
        new_end         = base_date + timedelta(days=DAYS_VIP)
        is_renewal      = (previous_status == "vip" and old_end_aware and old_end_aware > now)

        member_already_in_group = is_renewal and is_member_in_group(user_id)

        link = None
        if not member_already_in_group:
            try:
                bot.unban_chat_member(ID_GRUPO_VIP, int(user_id))
            except Exception:
                pass
            link_obj = create_invite_link(user_id, days=DAYS_VIP)
            link = link_obj.invite_link if link_obj else None

        bot_upsert_user(user_id, {
            "status":           "vip",
            "ja_pagou_vip":     True,
            "start_date":       base_date.isoformat(),
            "end_date":         new_end.isoformat(),
            "invite_link_url":  link if link else user_data.get("invite_link_url"),
            "last_payment_id":  payment_id,
            "renewal_notified": False,
            "teste_usado":      user_data.get("teste_usado", True),
            "processed_payments": [payment_id],
        })

        if is_renewal:
            if member_already_in_group:
                msg_texto = (f"*✅ RENOVACAO CONFIRMADA!*\n\n"
                             f"🎉 Voce *permanece no grupo* normalmente.\n"
                             f"📅 +{DAYS_VIP} dias adicionados.\n"
                             f"🗓️ Novo vencimento: *{new_end.strftime('%d/%m/%Y as %H:%M')}*")
            else:
                msg_texto = (f"*✅ RENOVACAO CONFIRMADA!*\n\n"
                             f"📅 +{DAYS_VIP} dias adicionados.\n"
                             f"🗓️ Novo vencimento: *{new_end.strftime('%d/%m/%Y as %H:%M')}*\n\n"
                             f"👉 *Clique no link para voltar ao grupo:*\n{link or '⚠️ Erro ao gerar link.'}")
            log_event = "RENOVOU_VIP"
        else:
            msg_texto = (f"*✅ PAGAMENTO APROVADO!*\n\n"
                         f"🎉 Bem-vindo ao VIP BazukaBets!\n"
                         f"🗓️ Acesso expira em: *{new_end.strftime('%d/%m/%Y as %H:%M')}*\n\n"
                         f"👉 *Clique no link para entrar no grupo:*\n{link or '⚠️ Erro ao gerar link.'}")
            log_event = "ENTROU_VIP"

        try:
            bot.send_message(int(user_id), msg_texto, parse_mode="Markdown")
        except Exception as e:
            logging.error(f"Erro ao enviar confirmacao VIP para {user_id}: {e}")

        msg_pix_id = user_data.get("last_pix_msg_id")
        if msg_pix_id:
            try:
                bot.delete_message(int(user_id), msg_pix_id)
            except Exception:
                pass

        try:
            log_to_channel(user_id, username, log_event, status="vip",
                           end_date=new_end.strftime("%d/%m/%Y %H:%M"))
        except Exception:
            pass

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logging.error(f"Erro no mercadopago_webhook: {e}")
        return jsonify({"status": "internal_error"}), 500


# ══════════════════════════════════════════════════════════════════════════════
# ROTAS FLASK — VIP PELO SITE
# ══════════════════════════════════════════════════════════════════════════════

def _generate_vip_code(payment_id: str) -> str:
    h    = hashlib.sha256(f"{payment_id}{SITE_SECRET_KEY}".encode()).hexdigest()
    code = h[:8].upper()
    return f"BBV-{code}"


def _parse_external_reference(ext_ref: str):
    raw = str(ext_ref).strip()
    if not raw.startswith("SITEVIP|"):
        return None, None
    parts = raw.split("|", 2)
    email       = parts[1].strip().lower() if len(parts) > 1 else ""
    telegram_id = parts[2].strip()         if len(parts) > 2 else ""
    return email, telegram_id


def _has_previous_vip(telegram_id: str) -> bool:
    if not telegram_id:
        return False
    user_data = bot_get_user(telegram_id)
    if user_data and (user_data.get('ja_pagou_vip') or user_data.get('status') == 'vip'):
        return True
    with get_db() as conn:
        row = conn.execute("SELECT id FROM vip_payments WHERE telegram_id=? AND status='approved'",
                           (telegram_id,)).fetchone()
        if row:
            return True
    return False


def _telegram_id_exists(telegram_id: str):
    if not telegram_id or not telegram_id.isdigit():
        return False, "ID invalido"
    user_data = bot_get_user(telegram_id)
    if user_data:
        return True, "ok"
    try:
        bot.get_chat(int(telegram_id))
        return True, "ok"
    except Exception:
        return False, "ID nao encontrado no Telegram"


def _send_vip_email(email: str, vip_code: str):
    if not SMTP_HOST or not SMTP_USER:
        return False, "smtp_not_configured"
    try:
        msg             = EmailMessage()
        msg['From']     = SMTP_FROM
        msg['To']       = email
        msg['Subject']  = 'BazukaBets VIP - pagamento aprovado'
        msg.set_content(
            f"Seu pagamento do VIP BazukaBets foi aprovado.\n\n"
            f"Codigo de acesso: {vip_code}\n\n"
            f"Como usar:\n"
            f"1. Abra o bot @BazukaBetsBot no Telegram\n"
            f"2. Cole o codigo acima no chat\n"
            f"3. O bot libera seu acesso automaticamente.\n\n"
            f"Qualquer duvida, fale com o suporte."
        )
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            if SMTP_USE_TLS:
                s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.send_message(msg)
        return True, "sent"
    except Exception as e:
        return False, f"error: {e}"


def _finalize_site_vip_payment(payment_id: str, info_mp: dict):
    """Processa pagamento aprovado pelo site (external_reference começa com SITEVIP|)."""
    if info_mp.get("status") != "approved":
        return jsonify({"status": "waiting"}), 200

    ext_ref = str(info_mp.get("external_reference", ""))
    email, telegram_id = _parse_external_reference(ext_ref)
    payer_email = str(info_mp.get("payer", {}).get("email", "")).strip().lower()
    email = payer_email or email
    if not email:
        return jsonify({"status": "missing_email"}), 400

    with get_db() as conn:
        existing = conn.execute(
            "SELECT * FROM vip_payments WHERE mp_payment_id=? AND status='approved'",
            (payment_id,)).fetchone()
        if existing:
            return jsonify({"status": "already_processed", "vip_code": existing["vip_code"]}), 200

        vip_code = _generate_vip_code(payment_id)
        conn.execute('''
            UPDATE vip_payments SET status='approved',
                telegram_id=COALESCE(NULLIF(telegram_id,''),?),
                email=COALESCE(NULLIF(email,''),?), vip_code=?, approved_at=?
            WHERE mp_payment_id=?
        ''', (telegram_id, email, vip_code, datetime.utcnow().isoformat(), payment_id))
        if not conn.execute("SELECT id FROM vip_payments WHERE mp_payment_id=?", (payment_id,)).fetchone():
            conn.execute('''
                INSERT INTO vip_payments (id,telegram_id,email,mp_payment_id,status,vip_code,created_at,approved_at)
                VALUES (?,?,?,?,'approved',?,?,?)
            ''', (uuid.uuid4().hex[:10], telegram_id, email, payment_id, vip_code,
                  datetime.utcnow().isoformat(), datetime.utcnow().isoformat()))
        conn.commit()

    # Salva na tabela vip_codes para o bot resgatar
    vip_code_save(vip_code, telegram_id, email, payment_id)

    # Notifica no Telegram se tiver telegram_id
    if telegram_id:
        try:
            bot.send_message(int(telegram_id),
                f"*🎉 PAGAMENTO APROVADO NO SITE!*\n\n"
                f"Seu codigo de acesso VIP é:\n\n"
                f"*`{vip_code}`*\n\n"
                f"📋 Cole este codigo aqui no chat para liberar seu acesso!\n"
                f"_(Basta copiar e colar, sem necessidade de nenhum comando)_")
        except Exception as e:
            logging.error(f"Nao foi possivel notificar {telegram_id}: {e}")

    _send_vip_email(email, vip_code)
    return jsonify({"status": "approved", "vip_code": vip_code}), 200


@app.route('/vip')
def vip_checkout():
    return render_template('vip_checkout.html')


@app.route('/api/vip/create_payment', methods=['POST'])
def create_vip_payment():
    data        = request.get_json(silent=True) or {}
    email       = str(data.get('email', '')).strip().lower()
    telegram_id = str(data.get('telegram_id', '')).strip()

    if not email or '@' not in email:
        return jsonify({'error': 'Informe um email valido.'}), 400
    if telegram_id and not telegram_id.isdigit():
        return jsonify({'error': 'O ID do Telegram deve conter apenas numeros.'}), 400

    valor_final    = float(VALOR_VIP_RECORRENTE)
    pricing_reason = 'recorrencia'
    if telegram_id:
        telegram_ok, _ = _telegram_id_exists(telegram_id)
        if not telegram_ok:
            return jsonify({'error': 'Nao foi possivel validar esse ID do Telegram. Abra o bot, envie /start e tente novamente.'}), 400
        if not _has_previous_vip(telegram_id):
            valor_final    = float(VALOR_VIP)
            pricing_reason = 'primeiro_mes'

    ext_ref = f"SITEVIP|{email}|{telegram_id}"
    payment_data = {
        "transaction_amount": valor_final,
        "description":        "Assinatura VIP BazukaBets - Site",
        "payment_method_id":  "pix",
        "payer": {"email": email, "first_name": "Cliente", "last_name": "BazukaBets"},
        "external_reference": ext_ref,
    }
    if NOTIFICATION_URL_SITE:
        payment_data["notification_url"] = NOTIFICATION_URL_SITE

    try:
        response = mp_sdk.payment().create(payment_data)
        if response.get("status") != 201:
            raise Exception(f"MP retornou: {response.get('status')}")

        resp_data = response["response"]
        mp_id     = str(resp_data["id"])
        qr_info   = resp_data["point_of_interaction"]["transaction_data"]
        pix_code  = qr_info.get("qr_code")
        qr_base64 = qr_info.get("qr_code_base64", "")

        with get_db() as conn:
            conn.execute('''
                INSERT INTO vip_payments (id,telegram_id,email,mp_payment_id,status,created_at)
                VALUES (?,?,?,?,'pending',?)
            ''', (uuid.uuid4().hex[:10], telegram_id or None, email, mp_id,
                  datetime.utcnow().isoformat()))
            conn.commit()

        return jsonify({
            'success':            True,
            'payment_id':         mp_id,
            'pix_code':           pix_code,
            'qr_base64':          qr_base64,
            'valor':              valor_final,
            'pricing_reason':     pricing_reason,
            'webhook_configured': bool(NOTIFICATION_URL_SITE),
        })

    except Exception as e:
        return jsonify({'error': f'Erro ao gerar pagamento: {str(e)}'}), 500


@app.route('/api/vip/webhook', methods=['POST'])
def vip_mp_webhook():
    try:
        data       = request.get_json(silent=True) or {}
        payment_id = None
        resource_url = data.get("resource")
        if resource_url and "/payments/" in str(resource_url):
            raw_id     = str(resource_url).split("/payments/")[-1].strip()
            payment_id = "".join(filter(str.isdigit, raw_id))
        if not payment_id and isinstance(data.get("data"), dict):
            payment_id = str(data["data"].get("id", ""))
        if not payment_id:
            return jsonify({"status": "no_id"}), 400

        url    = f"https://api.mercadopago.com/v1/payments/{payment_id}"
        resp   = requests.get(url, headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}"}, timeout=10)
        if not resp.ok:
            return jsonify({"status": "mp_error"}), 500

        info_mp = resp.json()
        ext_ref = str(info_mp.get("external_reference", ""))
        if not ext_ref.startswith("SITEVIP|"):
            return jsonify({"status": "not_site_payment"}), 200

        return _finalize_site_vip_payment(payment_id, info_mp)

    except Exception as e:
        logging.error(f"Erro no vip_mp_webhook: {e}")
        return jsonify({"status": "internal_error"}), 500


@app.route('/api/vip/check_payment/<payment_id>', methods=['GET'])
def check_vip_payment(payment_id):
    with get_db() as conn:
        row = conn.execute(
            "SELECT status, vip_code FROM vip_payments WHERE mp_payment_id=?",
            (payment_id,)).fetchone()
    if not row:
        return jsonify({"status": "not_found"}), 404
    if row["status"] != "approved":
        url  = f"https://api.mercadopago.com/v1/payments/{payment_id}"
        resp = requests.get(url, headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}"}, timeout=10)
        if resp.ok:
            info_mp = resp.json()
            if info_mp.get("status") == "approved":
                result, _ = _finalize_site_vip_payment(payment_id, info_mp)
                result_data = result.get_json()
                if result_data.get("status") == "approved":
                    return jsonify(result_data)
    return jsonify({"status": row["status"], "vip_code": row["vip_code"]})


@app.route('/api/admin/vip_payments')
def admin_vip_payments():
    if not session.get('logged_in') or session.get('role') != 'admin':
        return jsonify({'error': 'Forbidden'}), 403
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute(
            'SELECT * FROM vip_payments ORDER BY created_at DESC').fetchall()]
    return jsonify(rows)


# ══════════════════════════════════════════════════════════════════════════════
# ROTA DE IMPORTAÇÃO DO payments.json LEGADO
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/admin/import_payments_json', methods=['POST'])
def import_payments_json():
    """
    Importa dados do payments.json legado para o banco SQLite.
    Envie o arquivo via multipart/form-data com o campo 'file'.
    Protegida por autenticação admin.
    """
    if not session.get('logged_in') or session.get('role') != 'admin':
        return jsonify({'error': 'Forbidden'}), 403

    if 'file' not in request.files:
        return jsonify({'error': 'Envie o arquivo payments.json no campo "file"'}), 400

    raw = request.files['file'].read()
    try:
        data = json.loads(raw)
    except Exception as e:
        return jsonify({'error': f'JSON invalido: {e}'}), 400

    imported = updated = skipped = 0
    for user_id, info in data.items():
        if user_id == "config_bot_parado":
            bot_set_config('bot_parado', 'true' if info else 'false')
            continue
        if not isinstance(info, dict):
            continue
        existing = bot_get_user(user_id)
        record = {
            "status":           info.get("status", "novo"),
            "start_date":       info.get("start_date"),
            "end_date":         info.get("end_date"),
            "invite_link_url":  info.get("invite_link_url"),
            "last_payment_id":  info.get("last_payment_id"),
            "teste_usado":      bool(info.get("teste_usado", False)),
            "ja_pagou_vip":     bool(info.get("ja_pagou_vip", False)),
            "renewal_notified": bool(info.get("renewal_notified", False)),
            "last_pix_msg_id":  info.get("last_pix_msg_id"),
            "origem":           info.get("origem", "bot"),
            "processed_payments": info.get("processed_payments", []),
        }
        bot_upsert_user(user_id, record)
        if existing:
            updated += 1
        else:
            imported += 1

    return jsonify({
        "success":  True,
        "imported": imported,
        "updated":  updated,
        "skipped":  skipped,
        "total":    imported + updated,
    })


# ══════════════════════════════════════════════════════════════════════════════
# BUSINESS LOGIC DO SITE (apostas, stats, etc.)
# ══════════════════════════════════════════════════════════════════════════════

INVALID_RESULTS = {'Reembolso', 'Pendente', '-', '', 'nan', 'None'}
RESULTADO_ALIASES = {
    'green': 'Green', '✅ green': 'Green',
    'meio green': 'Meio Green', '🔁✅ meio green': 'Meio Green',
    'red': 'Red', '❌ red': 'Red',
    'meio red': 'Meio Red', '🔁❌ meio red': 'Meio Red',
    'reembolso': 'Reembolso', '🔄 reembolso': 'Reembolso',
    'pendente': 'Pendente',
}
EXCEL_EPOCH = pd.Timestamp('1899-12-30')


def calc_lucro(resultado, odd):
    r = normalize_result(resultado)
    if r == 'Green':      return round(float(odd) - 1, 4)
    if r == 'Meio Green': return round((float(odd) - 1) / 2, 4)
    if r == 'Red':        return -1.0
    if r == 'Meio Red':   return -0.5
    return 0.0


def normalize_result(resultado):
    raw     = str(resultado).strip()
    cleaned = ' '.join(
        raw.replace('☑',' ').replace('✅',' ').replace('❌',' ')
           .replace('✖',' ').replace('🔁',' ').replace('🔄',' ').split()
    ).lower()
    mapping = {'green':'Green','meio green':'Meio Green','red':'Red',
               'meio red':'Meio Red','reembolso':'Reembolso','pendente':'Pendente'}
    return mapping.get(cleaned, RESULTADO_ALIASES.get(raw.lower(), raw))


def excel_serial_to_datetime(value):
    try:
        return EXCEL_EPOCH + pd.to_timedelta(float(value), unit='D')
    except Exception:
        return None


def get_faixa(hora):
    try:
        h = int(str(hora).split(':')[0])
        if h < 4:  return '00:00-03:59'
        if h < 8:  return '04:00-07:59'
        if h < 12: return '08:00-11:59'
        if h < 16: return '12:00-15:59'
        if h < 20: return '16:00-19:59'
        return '20:00-23:59'
    except Exception:
        return '00:00-03:59'


def parse_mes_key(data_str):
    try:
        p = str(data_str).split('/')
        if len(p) == 3 and len(p[2]) == 4:
            return f"{p[1]}/{p[2]}"
    except Exception:
        pass
    return None


def sort_mes_key(mes_str):
    try:
        p = mes_str.split('/')
        return (int(p[1]), int(p[0]))
    except Exception:
        return (0, 0)


def rows_for_mes(conn, mes):
    all_rows = conn.execute('SELECT * FROM apostas').fetchall()
    return [dict(r) for r in all_rows if parse_mes_key(r['data']) == mes]


def rows_for_period(conn, di_str, df_str):
    try:
        d1, d2 = datetime.strptime(di_str,'%d/%m/%Y'), datetime.strptime(df_str,'%d/%m/%Y')
        if d1 > d2: d1, d2 = d2, d1
    except Exception:
        return []
    result = []
    for row in [dict(r) for r in conn.execute('SELECT * FROM apostas').fetchall()]:
        try:
            if d1 <= datetime.strptime(row['data'],'%d/%m/%Y') <= d2:
                result.append(row)
        except Exception:
            pass
    return result


def calc_stats(rows):
    if not rows:
        return {'entradas':0,'greens':0,'reds':0,'lucro':0,'roi':0,'win_rate':0,'odd_media':0}
    validos     = [r for r in rows if str(r['resultado']).strip() not in INVALID_RESULTS]
    greens      = sum(1 for r in rows if r['resultado'] == 'Green')
    meio_greens = sum(1 for r in rows if r['resultado'] == 'Meio Green')
    reds        = sum(1 for r in rows if r['resultado'] == 'Red')
    meio_reds   = sum(1 for r in rows if r['resultado'] == 'Meio Red')
    lucro_total = sum(r['lucro'] for r in rows)
    gw, ls      = greens + meio_greens, reds + meio_reds
    roi         = round(lucro_total / len(validos) * 100, 2) if validos else 0
    win_rate    = round(gw / (gw + ls) * 100, 2) if (gw + ls) else 0
    odd_media   = round(sum(r['odd'] for r in rows) / len(rows), 2) if rows else 0
    return {'entradas':len(rows),'greens':gw,'reds':ls,'lucro':round(lucro_total,2),
            'roi':roi,'win_rate':win_rate,'odd_media':odd_media}


def calc_monthly(rows):
    months = defaultdict(list)
    for r in rows:
        key = parse_mes_key(r['data'])
        if key: months[key].append(r)
    result = []
    for m in sorted(months.keys(), key=sort_mes_key):
        mrs     = months[m]
        validos = [r for r in mrs if str(r['resultado']).strip() not in INVALID_RESULTS]
        lucro   = round(sum(r['lucro'] for r in mrs), 2)
        entradas= len(mrs)
        roi     = round(lucro / len(validos) * 100, 2) if validos else 0
        odd_m   = round(sum(r['odd'] for r in mrs) / len(mrs), 2) if mrs else 0
        result.append({'mes':m,'entradas':entradas,'retorno':round(entradas+lucro,2),
                       'resultado':lucro,'odd_media':odd_m,'roi':roi})
    return result


def calc_annual(rows):
    years = defaultdict(list)
    for r in rows:
        try:
            p = str(r['data']).split('/')
            if len(p) == 3: years[p[2]].append(r)
        except Exception: pass
    return [{'ano':y,'resultado':round(sum(r['lucro'] for r in yrs),2)}
            for y, yrs in sorted(years.items())]


def calc_diario(rows):
    days = defaultdict(list)
    for r in rows: days[r['data']].append(r)
    def day_sort(d):
        try: return datetime.strptime(d,'%d/%m/%Y')
        except: return datetime.min
    result = []
    for d in sorted(days.keys(), key=day_sort):
        drs     = days[d]
        validos = [r for r in drs if str(r['resultado']).strip() not in INVALID_RESULTS]
        lucro   = round(sum(r['lucro'] for r in drs), 2)
        roi     = round(lucro / len(validos) * 100, 2) if validos else 0
        result.append({'data':d,'entradas':len(drs),'lucro':lucro,'roi':roi})
    return result


def calc_faixas(rows):
    ORDER  = ['00:00-03:59','04:00-07:59','08:00-11:59','12:00-15:59','16:00-19:59','20:00-23:59']
    faixas = defaultdict(list)
    for r in rows: faixas[r['faixa_horaria']].append(r)
    result = []
    for f in ORDER:
        frs     = faixas.get(f, [])
        validos = [r for r in frs if str(r['resultado']).strip() not in INVALID_RESULTS]
        lucro   = round(sum(r['lucro'] for r in frs), 2)
        roi     = round(lucro / len(validos) * 100, 2) if validos else 0
        result.append({'faixa':f,'entradas':len(frs),'resultado':lucro,'roi':roi})
    return result


def calc_ligas(rows):
    ligas = defaultdict(list)
    for r in rows: ligas[r['liga']].append(r)
    result = []
    for l, lrs in sorted(ligas.items(), key=lambda x: -sum(r['lucro'] for r in x[1])):
        validos = [r for r in lrs if str(r['resultado']).strip() not in INVALID_RESULTS]
        lucro   = round(sum(r['lucro'] for r in lrs), 2)
        roi     = round(lucro / len(validos) * 100, 2) if validos else 0
        result.append({'liga':l,'entradas':len(lrs),'resultado':lucro,'roi':roi})
    return result


def calc_liga_mercado(rows):
    lm = defaultdict(list)
    for r in rows:
        lm[f"{r['liga']} {r['mercado']}"].append(r)
    result = []
    for k, krs in sorted(lm.items(), key=lambda x: -sum(r['lucro'] for r in x[1])):
        validos = [r for r in krs if str(r['resultado']).strip() not in INVALID_RESULTS]
        lucro   = round(sum(r['lucro'] for r in krs), 2)
        roi     = round(lucro / len(validos) * 100, 2) if validos else 0
        result.append({'liga_mercado':k,'entradas':len(krs),'resultado':lucro,'roi':roi})
    return result


def calc_rankings(rows):
    jogadores  = defaultdict(lambda: {'lucro':0.0,'apostas':0})
    confrontos = defaultdict(lambda: {'lucro':0.0,'partidas':0})
    for r in rows:
        c, lucro = str(r['confronto']), r['lucro']
        if ' vs ' in c:
            j1, j2 = [x.strip() for x in c.split(' vs ', 1)]
            for j in [j1, j2]:
                jogadores[j]['lucro']  += lucro
                jogadores[j]['apostas'] += 1
            key = ' vs '.join(sorted([j1, j2]))
            confrontos[key]['lucro']   += lucro
            confrontos[key]['partidas'] += 1
    sj = sorted(jogadores.items(),  key=lambda x: x[1]['lucro'], reverse=True)
    sc = sorted(confrontos.items(), key=lambda x: x[1]['lucro'], reverse=True)
    fj = lambda items: [{'nome':k,'lucro':round(v['lucro'],2),'apostas':v['apostas']} for k,v in items]
    fc = lambda items: [{'confronto':k,'lucro':round(v['lucro'],2),'partidas':v['partidas']} for k,v in items]
    return {'top_jogadores':fj(sj[:5]),'bot_jogadores':fj(sj[-5:][::-1]) if len(sj)>=5 else fj(list(reversed(sj))),
            'top_confrontos':fc(sc[:5]),'bot_confrontos':fc(sc[-5:][::-1]) if len(sc)>=5 else fc(list(reversed(sc)))}


def calc_banca_stats(rows):
    if not rows:
        return {'entradas':0,'total_apostado':0,'retorno_total':0,'lucro_rs':0,'lucro_un':0,'roi':0,'win_rate':0}
    greens = sum(1 for r in rows if r['resultado'] in ('Green','Meio Green'))
    reds   = sum(1 for r in rows if r['resultado'] in ('Red','Meio Red'))
    total_apostado = round(sum(float(r['valor_apostado']) for r in rows), 2)
    lucro_rs       = round(sum(float(r['lucro']) for r in rows), 2)
    lucro_un       = round(sum(calc_lucro(r['resultado'], r['odd']) for r in rows), 2)
    roi            = round((lucro_rs / total_apostado) * 100, 2) if total_apostado else 0
    win_rate       = round((greens / (greens + reds)) * 100, 2) if (greens + reds) else 0
    return {'entradas':len(rows),'total_apostado':total_apostado,
            'retorno_total':round(total_apostado+lucro_rs,2),
            'lucro_rs':lucro_rs,'lucro_un':lucro_un,'roi':roi,'win_rate':win_rate}


# ── Auth ──────────────────────────────────────────────────────────────────────

def parse_iso_datetime(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def calculate_active_until(days):
    if days in (None, '', 0, '0'):
        return None
    try:
        days = int(days)
    except Exception:
        return None
    if days <= 0:
        return None
    return (datetime.utcnow() + timedelta(days=days)).replace(microsecond=0).isoformat()


def sync_user_status(conn, user_row):
    if not user_row:
        return user_row
    user    = dict(user_row)
    if user.get('role') == 'admin':
        return user
    changed = False
    active_until = parse_iso_datetime(user.get('active_until'))
    if user.get('status') == 'active' and active_until and active_until < datetime.utcnow():
        user['status'] = 'standby'
        changed = True
    if user.get('approved') and not user.get('status'):
        user['status'] = 'active'
        changed = True
    if changed:
        conn.execute('UPDATE users SET status=? WHERE id=?', (user['status'], user['id']))
        conn.commit()
    return user


def current_user():
    if not session.get('logged_in'):
        return None
    return {'id':session.get('user_id'),'role':session.get('role','admin'),
            'nome':session.get('nome'),'email':session.get('email'),
            'status':session.get('status','active'),'active_until':session.get('active_until')}


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in'):
            return jsonify({'error': 'Unauthorized'}), 401
        if session.get('role') != 'admin':
            with get_db() as conn:
                user = conn.execute('SELECT * FROM users WHERE id=?', (session.get('user_id'),)).fetchone()
                user = sync_user_status(conn, user)
            if not user or not user.get('approved'):
                session.clear()
                return jsonify({'error': 'Sua conta nao esta liberada.'}), 403
            if user.get('status') == 'paused':
                session.clear()
                return jsonify({'error': 'Sua conta esta pausada.'}), 403
            if user.get('status') == 'standby':
                session.clear()
                return jsonify({'error': 'Sua conta esta em stand by aguardando renovacao.'}), 403
            session['status']       = user.get('status', 'active')
            session['active_until'] = user.get('active_until')
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('logged_in') or session.get('role') != 'admin':
            return jsonify({'error': 'Forbidden'}), 403
        return f(*args, **kwargs)
    return decorated


# ══════════════════════════════════════════════════════════════════════════════
# ROTAS FLASK — SITE (auth, dashboard, admin)
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/apresentacao')
def apresentacao():
    return render_template('apresentacao.html')


@app.route('/api/register', methods=['POST'])
def register():
    data  = request.get_json(silent=True) or {}
    nome  = str(data.get('nome',     '')).strip()
    email = str(data.get('email',    '')).strip().lower()
    senha = str(data.get('password', '')).strip()
    if not nome or not email or not senha:
        return jsonify({'success': False, 'error': 'Preencha nome, email e senha.'}), 400
    with get_db() as conn:
        if conn.execute('SELECT id FROM users WHERE email=?', (email,)).fetchone():
            return jsonify({'success': False, 'error': 'Este email ja esta cadastrado.'}), 400
        conn.execute('''
            INSERT INTO users (id,nome,email,password_hash,role,approved,status,created_at)
            VALUES (?,?,?,?,?,?,?,?)
        ''', (uuid.uuid4().hex[:12], nome, email, generate_password_hash(senha),
              'user', 0, 'pending', datetime.utcnow().isoformat()))
        conn.commit()
    return jsonify({'success': True, 'message': 'Cadastro enviado para aprovacao.'})


@app.route('/api/login', methods=['POST'])
def login():
    data     = request.get_json(silent=True) or {}
    username = str(data.get('username', '')).strip().lower()
    password = str(data.get('password', ''))
    with get_db() as conn:
        user = conn.execute('SELECT * FROM users WHERE lower(email)=?', (username,)).fetchone()
        user = sync_user_status(conn, user)
    if user and check_password_hash(user['password_hash'], password):
        if user['role'] != 'admin' and not user['approved']:
            return jsonify({'success': False, 'error': 'Cadastro ainda nao aprovado.'}), 403
        if user['role'] != 'admin' and user.get('status') == 'paused':
            return jsonify({'success': False, 'error': 'Conta pausada. Fale com o administrador.'}), 403
        if user['role'] != 'admin' and user.get('status') == 'standby':
            return jsonify({'success': False, 'error': 'Conta em stand by aguardando renovacao.'}), 403
        session.update({'logged_in': True, 'user_id': user['id'], 'role': user['role'],
                        'nome': user['nome'], 'email': user['email'],
                        'status': user.get('status', 'active'), 'active_until': user.get('active_until')})
        return jsonify({'success': True, 'user': {'role': user['role'], 'nome': user['nome'],
                        'email': user['email'], 'status': user.get('status', 'active'),
                        'active_until': user.get('active_until')}})
    return jsonify({'success': False, 'error': 'Credenciais invalidas'}), 401


@app.route('/api/logout', methods=['POST'])
def logout():
    session.clear()
    return jsonify({'success': True})


@app.route('/api/check_auth')
def check_auth():
    if session.get('logged_in') and session.get('role') != 'admin':
        with get_db() as conn:
            user = conn.execute('SELECT * FROM users WHERE id=?', (session.get('user_id'),)).fetchone()
            user = sync_user_status(conn, user)
        if not user or not user.get('approved') or user.get('status') in {'paused', 'standby'}:
            session.clear()
    return jsonify({'logged_in': bool(session.get('logged_in')), 'user': current_user()})


@app.route('/api/dashboard')
def dashboard():
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute('SELECT * FROM apostas').fetchall()]
    return jsonify({'stats': calc_stats(rows), 'monthly': calc_monthly(rows),
                    'annual': calc_annual(rows), 'faixas': calc_faixas(rows),
                    'ligas': calc_ligas(rows), 'liga_mercado': calc_liga_mercado(rows),
                    'rankings': calc_rankings(rows)})


@app.route('/api/meses')
def meses():
    with get_db() as conn:
        rows = conn.execute('SELECT DISTINCT data FROM apostas').fetchall()
    meses_set = set()
    for r in rows:
        key = parse_mes_key(r['data'])
        if key: meses_set.add(key)
    return jsonify(sorted(list(meses_set), key=sort_mes_key))


@app.route('/api/mensal/<path:mes>')
def mensal(mes):
    with get_db() as conn:
        rows = rows_for_mes(conn, mes)
    return jsonify({'stats': calc_stats(rows), 'diario': calc_diario(rows),
                    'faixas': calc_faixas(rows), 'ligas': calc_ligas(rows),
                    'liga_mercado': calc_liga_mercado(rows), 'rankings': calc_rankings(rows)})


@app.route('/api/periodo')
def periodo():
    di = request.args.get('data_inicial', '')
    df = request.args.get('data_final', '')
    if not di or not df:
        return jsonify({'error': 'Informe data inicial e data final.'}), 400
    with get_db() as conn:
        rows = rows_for_period(conn, di, df)
    return jsonify({'stats': calc_stats(rows), 'diario': calc_diario(rows),
                    'faixas': calc_faixas(rows), 'ligas': calc_ligas(rows),
                    'liga_mercado': calc_liga_mercado(rows), 'rankings': calc_rankings(rows),
                    'label': f'{di} - {df}', 'total': len(rows)})


@app.route('/api/rankings_filter')
def rankings_filter():
    mercado = request.args.get('mercado', 'Todos')
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute('SELECT * FROM apostas').fetchall()]
    if mercado != 'Todos':
        rows = [r for r in rows if r['mercado'] == mercado]
    return jsonify(calc_rankings(rows))


@app.route('/api/rankings_filter_mes')
def rankings_filter_mes():
    mercado = request.args.get('mercado', 'Todos')
    mes     = request.args.get('mes', '')
    with get_db() as conn:
        rows = rows_for_mes(conn, mes)
    if mercado != 'Todos':
        rows = [r for r in rows if r['mercado'] == mercado]
    return jsonify(calc_rankings(rows))


@app.route('/api/rankings_filter_periodo')
def rankings_filter_periodo():
    mercado = request.args.get('mercado', 'Todos')
    di = request.args.get('data_inicial', '')
    df = request.args.get('data_final', '')
    with get_db() as conn:
        rows = rows_for_period(conn, di, df)
    if mercado != 'Todos':
        rows = [r for r in rows if r['mercado'] == mercado]
    return jsonify(calc_rankings(rows))


@app.route('/api/dados_brutos')
def dados_brutos():
    search   = request.args.get('search', '').strip()
    mercado  = request.args.get('mercado', '')
    page     = max(1, int(request.args.get('page', 1)))
    per_page = 50
    base, params = 'FROM apostas WHERE 1=1', []
    if search:
        base += ' AND (confronto LIKE ? OR liga LIKE ?)'
        params += [f'%{search}%', f'%{search}%']
    if mercado and mercado != 'Todos':
        base += ' AND mercado=?'
        params.append(mercado)
    with get_db() as conn:
        total = conn.execute(f'SELECT COUNT(*) {base}', params).fetchone()[0]
        rows  = [dict(r) for r in conn.execute(
            f'SELECT * {base} ORDER BY data, hora LIMIT {per_page} OFFSET {(page-1)*per_page}',
            params).fetchall()]
    return jsonify({'data': rows, 'total': total, 'page': page, 'per_page': per_page})


@app.route('/api/upload', methods=['POST'])
@admin_required
def upload():
    if 'file' not in request.files:
        return jsonify({'error': 'Nenhum arquivo enviado'}), 400
    f = request.files['file']
    if not f.filename.lower().endswith('.xlsx'):
        return jsonify({'error': 'Apenas arquivos .xlsx sao suportados'}), 400
    file_bytes = BytesIO(f.read())
    try:
        df = pd.read_excel(file_bytes)
        df.columns = [str(c).strip() for c in df.columns]
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if 'liga' in cl and 'Liga' not in col_map:                                 col_map['Liga'] = c
            elif 'data' in cl and 'Data' not in col_map:                               col_map['Data'] = c
            elif 'hora' in cl and 'Hora' not in col_map:                               col_map['Hora'] = c
            elif ('confronto' in cl or 'match' in cl) and 'Confronto' not in col_map: col_map['Confronto'] = c
            elif ('mercado' in cl or 'market' in cl) and 'Mercado' not in col_map:    col_map['Mercado'] = c
            elif 'estrat' in cl and 'Mercado' not in col_map:                          col_map['Mercado'] = c
            elif ('jogador 1' in cl or 'jogador1' in cl) and 'Jogador1' not in col_map: col_map['Jogador1'] = c
            elif ('jogador 2' in cl or 'jogador2' in cl) and 'Jogador2' not in col_map: col_map['Jogador2'] = c
            elif 'odd' in cl and 'Odd' not in col_map:                                col_map['Odd'] = c
            elif ('saldo' in cl or 'lucro' in cl or 'profit' in cl) and 'Saldo' not in col_map: col_map['Saldo'] = c
            elif 'result' in cl and 'Resultado' not in col_map:                        col_map['Resultado'] = c

        missing = [r for r in ['Liga','Data','Hora','Odd','Resultado'] if r not in col_map]
        if 'Confronto' not in col_map and ('Jogador1' not in col_map or 'Jogador2' not in col_map):
            missing.append('Confronto/Jogador1+Jogador2')
        if 'Mercado' not in col_map:
            missing.append('Mercado/Estrategia')
        if missing:
            return jsonify({'error': f'Colunas nao encontradas: {missing}'}), 400

        df = df.rename(columns={v: k for k, v in col_map.items()})
        if 'Confronto' not in df.columns:
            df['Confronto'] = df['Jogador1'].astype(str).str.strip() + ' vs ' + df['Jogador2'].astype(str).str.strip()

        df['Resultado'] = df['Resultado'].apply(normalize_result)
        df['Odd']       = pd.to_numeric(df['Odd'], errors='coerce').fillna(1.0)
        if 'Saldo' in df.columns:
            df['Saldo'] = pd.to_numeric(df['Saldo'], errors='coerce')

        def fmt_date(d):
            if pd.isna(d): return ''
            if isinstance(d, (pd.Timestamp, datetime)): return d.strftime('%d/%m/%Y')
            excel_dt = excel_serial_to_datetime(d)
            if excel_dt is not None: return excel_dt.strftime('%d/%m/%Y')
            s = str(d).strip().split(' ')[0]
            for fmt in ['%d/%m/%Y','%Y-%m-%d','%m/%d/%Y','%d-%m-%Y']:
                try: return datetime.strptime(s, fmt).strftime('%d/%m/%Y')
                except: pass
            return s

        def fmt_hora(h):
            if pd.isna(h): return '00:00'
            if isinstance(h, (pd.Timestamp, datetime)): return h.strftime('%H:%M')
            excel_dt = excel_serial_to_datetime(h)
            if excel_dt is not None: return excel_dt.strftime('%H:%M')
            s = str(h).strip()
            if ':' in s: return s[:5]
            return '00:00'

        inserted = 0
        with get_db() as conn:
            for _, row in df.iterrows():
                data_val  = fmt_date(row['Data'])
                hora_val  = fmt_hora(row['Hora'])
                resultado = normalize_result(row['Resultado'])
                odd       = float(row['Odd'])
                lucro     = float(row['Saldo']) if 'Saldo' in row and not pd.isna(row['Saldo']) else calc_lucro(resultado, odd)
                faixa     = get_faixa(hora_val)
                conn.execute('''
                    INSERT INTO apostas (id,liga,data,hora,confronto,mercado,odd,resultado,lucro,faixa_horaria)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                ''', (uuid.uuid4().hex[:8], row['Liga'], data_val, hora_val,
                      row['Confronto'], row['Mercado'], odd, resultado, lucro, faixa))
                inserted += 1
            conn.commit()
        return jsonify({'success': True, 'inserted': inserted})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/admin/clear', methods=['POST'])
@admin_required
def clear_all():
    with get_db() as conn:
        conn.execute('DELETE FROM apostas')
        conn.commit()
    return jsonify({'success': True})


@app.route('/api/admin/clear_period', methods=['POST'])
@admin_required
def clear_period():
    data = request.get_json(silent=True) or {}
    try:
        d1 = datetime.strptime(data.get('data_inicial',''), '%d/%m/%Y')
        d2 = datetime.strptime(data.get('data_final',''),   '%d/%m/%Y')
    except Exception:
        return jsonify({'error': 'Datas invalidas. Use dd/mm/yyyy'}), 400
    with get_db() as conn:
        rows   = conn.execute('SELECT id, data FROM apostas').fetchall()
        to_del = []
        for r in rows:
            try:
                if d1 <= datetime.strptime(r['data'],'%d/%m/%Y') <= d2:
                    to_del.append(r['id'])
            except Exception: pass
        if to_del:
            conn.execute(f"DELETE FROM apostas WHERE id IN ({','.join(['?']*len(to_del))})", to_del)
            conn.commit()
    return jsonify({'success': True, 'deleted': len(to_del)})


@app.route('/api/admin/apostas')
@admin_required
def admin_apostas():
    page     = max(1, int(request.args.get('page', 1)))
    per_page = 50
    with get_db() as conn:
        total = conn.execute('SELECT COUNT(*) FROM apostas').fetchone()[0]
        rows  = [dict(r) for r in conn.execute(
            f'SELECT * FROM apostas ORDER BY data, hora LIMIT {per_page} OFFSET {(page-1)*per_page}'
        ).fetchall()]
    return jsonify({'data': rows, 'total': total, 'page': page, 'per_page': per_page})


@app.route('/api/admin/aposta/<id>', methods=['DELETE'])
@admin_required
def delete_aposta(id):
    with get_db() as conn:
        conn.execute('DELETE FROM apostas WHERE id=?', (id,))
        conn.commit()
    return jsonify({'success': True})


@app.route('/api/admin/aposta/<id>', methods=['PUT'])
@admin_required
def update_aposta(id):
    data      = request.get_json(silent=True) or {}
    resultado = normalize_result(data.get('resultado',''))
    odd       = float(data.get('odd', 1))
    hora      = str(data.get('hora','')).strip()
    with get_db() as conn:
        conn.execute('''
            UPDATE apostas SET liga=?,data=?,hora=?,confronto=?,mercado=?,odd=?,resultado=?,lucro=?,faixa_horaria=?
            WHERE id=?
        ''', (data.get('liga',''), data.get('data',''), hora, data.get('confronto',''),
              data.get('mercado',''), odd, resultado, calc_lucro(resultado, odd), get_faixa(hora), id))
        conn.commit()
    return jsonify({'success': True})


@app.route('/api/minha_banca')
@login_required
def minha_banca():
    user = current_user()
    with get_db() as conn:
        rows = [dict(r) for r in conn.execute(
            'SELECT * FROM banca_apostas WHERE user_id=? ORDER BY data DESC, hora DESC, created_at DESC',
            (user['id'],)).fetchall()]
    return jsonify({'stats': calc_banca_stats(rows), 'apostas': rows})


@app.route('/api/minha_banca', methods=['POST'])
@login_required
def add_minha_banca():
    user = current_user()
    data = request.get_json(silent=True) or {}
    try:
        odd   = float(data.get('odd', 1) or 1)
        valor = float(data.get('valor_apostado', 0) or 0)
    except Exception:
        return jsonify({'error': 'Odd ou valor invalido.'}), 400
    resultado = normalize_result(data.get('resultado', 'Pendente'))
    lucro     = round(calc_lucro(resultado, odd) * valor, 2)
    with get_db() as conn:
        conn.execute('''
            INSERT INTO banca_apostas (id,user_id,liga,data,hora,confronto,mercado,odd,resultado,valor_apostado,lucro,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ''', (uuid.uuid4().hex[:10], user['id'], data.get('liga',''), data.get('data',''),
              data.get('hora',''), data.get('confronto',''), data.get('mercado',''),
              odd, resultado, valor, lucro, datetime.utcnow().isoformat()))
        conn.commit()
    return jsonify({'success': True})


@app.route('/api/minha_banca/<id>', methods=['PUT'])
@login_required
def update_minha_banca(id):
    user = current_user()
    data = request.get_json(silent=True) or {}
    try:
        odd   = float(data.get('odd', 1) or 1)
        valor = float(data.get('valor_apostado', 0) or 0)
    except Exception:
        return jsonify({'error': 'Odd ou valor invalido.'}), 400
    resultado = normalize_result(data.get('resultado', 'Pendente'))
    lucro     = round(calc_lucro(resultado, odd) * valor, 2)
    with get_db() as conn:
        conn.execute('''
            UPDATE banca_apostas SET liga=?,data=?,hora=?,confronto=?,mercado=?,odd=?,resultado=?,valor_apostado=?,lucro=?
            WHERE id=? AND user_id=?
        ''', (data.get('liga',''), data.get('data',''), data.get('hora',''), data.get('confronto',''),
              data.get('mercado',''), odd, resultado, valor, lucro, id, user['id']))
        conn.commit()
    return jsonify({'success': True})


@app.route('/api/minha_banca/<id>', methods=['DELETE'])
@login_required
def delete_minha_banca(id):
    user = current_user()
    with get_db() as conn:
        conn.execute('DELETE FROM banca_apostas WHERE id=? AND user_id=?', (id, user['id']))
        conn.commit()
    return jsonify({'success': True})


@app.route('/api/admin/users')
@admin_required
def admin_users():
    with get_db() as conn:
        pending_rows  = conn.execute(
            "SELECT id,nome,email,approved,status,active_until,paused_at,created_at,role FROM users WHERE role='user' AND approved=0 ORDER BY created_at"
        ).fetchall()
        approved_rows = conn.execute(
            "SELECT id,nome,email,approved,status,active_until,paused_at,created_at,role FROM users WHERE role='user' AND approved=1 ORDER BY created_at DESC"
        ).fetchall()
        pending  = [sync_user_status(conn, r) for r in pending_rows]
        approved = [sync_user_status(conn, r) for r in approved_rows]
    return jsonify({'pending': pending, 'approved': approved})


@app.route('/api/admin/users/<id>/approve', methods=['POST'])
@admin_required
def approve_user(id):
    data         = request.get_json(silent=True) or {}
    active_until = calculate_active_until(data.get('active_days'))
    with get_db() as conn:
        conn.execute(
            "UPDATE users SET approved=1, status='active', active_until=?, paused_at=NULL WHERE id=? AND role='user'",
            (active_until, id))
        conn.commit()
    return jsonify({'success': True})


@app.route('/api/admin/users/<id>', methods=['PUT'])
@admin_required
def update_user(id):
    data   = request.get_json(silent=True) or {}
    action = str(data.get('action', '')).strip().lower()
    with get_db() as conn:
        user = conn.execute("SELECT * FROM users WHERE id=? AND role='user'", (id,)).fetchone()
        if not user:
            return jsonify({'error': 'Usuario nao encontrado.'}), 404
        status       = user['status'] or ('active' if user['approved'] else 'pending')
        approved     = user['approved']
        paused_at    = user['paused_at']
        active_until = user['active_until']
        if data.get('active_days') not in (None, ''):
            active_until = calculate_active_until(data['active_days'])
        if action == 'pause':
            status    = 'paused'
            paused_at = datetime.utcnow().replace(microsecond=0).isoformat()
        elif action == 'activate':
            status   = 'active'
            approved = 1
            paused_at = None
        elif action == 'standby':
            status = 'standby'
        conn.execute('''
            UPDATE users SET nome=?,email=?,approved=?,status=?,active_until=?,paused_at=? WHERE id=? AND role='user'
        ''', (str(data.get('nome','') or user['nome']).strip(),
              str(data.get('email','') or user['email']).strip().lower(),
              approved, status, active_until, paused_at, id))
        conn.commit()
        updated = sync_user_status(conn, conn.execute(
            "SELECT id,nome,email,approved,status,active_until,paused_at,created_at,role FROM users WHERE id=?", (id,)
        ).fetchone())
    return jsonify({'success': True, 'user': updated})


@app.route('/api/admin/users/<id>', methods=['DELETE'])
@admin_required
def delete_user(id):
    with get_db() as conn:
        conn.execute("DELETE FROM banca_apostas WHERE user_id=?", (id,))
        conn.execute("DELETE FROM users WHERE id=? AND role='user'", (id,))
        conn.commit()
    return jsonify({'success': True})


# ══════════════════════════════════════════════════════════════════════════════
# INICIALIZAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

init_db()
threading.Thread(target=periodic_check, daemon=True).start()


if __name__ == "__main__":
    port  = int(os.environ.get('PORT', 8080))
    debug = os.environ.get('FLASK_DEBUG', '').lower() in {'1', 'true', 'yes'}

    logging.info("=== BazukaBets Sistema Unificado iniciando ===")
    logging.info(f"DB: {DB_PATH}")
    logging.info(f"SITE_PUBLIC_URL: {SITE_PUBLIC_URL or '(nao configurado)'}")

    # Em produção (Railway), usar webhook do Telegram via Flask.
    # Em desenvolvimento local, usar polling.
    USE_WEBHOOK = bool(SITE_PUBLIC_URL and not debug)

    if USE_WEBHOOK:
        webhook_url = f"{SITE_PUBLIC_URL}/telegram_webhook"
        logging.info(f"Configurando webhook Telegram: {webhook_url}")
        bot.remove_webhook()
        time.sleep(1)
        bot.set_webhook(url=webhook_url)
        logging.info("Webhook configurado. Iniciando Flask...")
        app.run(host="0.0.0.0", port=port, debug=False)
    else:
        logging.info("Modo desenvolvimento: Flask + polling do Telegram em threads separadas.")
        def run_flask():
            app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
        threading.Thread(target=run_flask, daemon=True).start()
        bot.remove_webhook()
        bot.infinity_polling(timeout=30, long_polling_timeout=30)
