#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
migrate_payments.py
====================
Importa os dados do payments.json legado para a tabela bot_payments do SQLite.
Execute uma única vez antes do primeiro deploy do main.py no Railway.

Uso:
    python migrate_payments.py [caminho_para_payments.json] [caminho_para_bazuka.db]

Exemplo:
    python migrate_payments.py payments.json bazuka.db
"""

import sys, json, sqlite3, os
from datetime import datetime

PAYMENTS_FILE = sys.argv[1] if len(sys.argv) > 1 else "payments.json"
DB_PATH       = sys.argv[2] if len(sys.argv) > 2 else "bazuka.db"

if not os.path.exists(PAYMENTS_FILE):
    print(f"[ERRO] Arquivo não encontrado: {PAYMENTS_FILE}")
    sys.exit(1)

if not os.path.exists(DB_PATH):
    print(f"[AVISO] Banco {DB_PATH} não existe ainda — será criado.")

print(f"\n{'='*55}")
print(f"  BazukaBets — Migração payments.json → SQLite")
print(f"{'='*55}")
print(f"  Origem : {PAYMENTS_FILE}")
print(f"  Destino: {DB_PATH}")
print(f"{'='*55}\n")

# ── Carrega o JSON ─────────────────────────────────────────────────────────

with open(PAYMENTS_FILE, 'r', encoding='utf-8') as f:
    data = json.load(f)

print(f"  Total de entradas no JSON: {len(data)}\n")

# ── Conecta ao banco ────────────────────────────────────────────────────────

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

# Garante que as tabelas existem (caso o banco seja novo)
conn.executescript('''
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
    );
    CREATE TABLE IF NOT EXISTS bot_processed_payments (
        user_id TEXT NOT NULL,
        payment_id TEXT NOT NULL,
        processed_at TEXT,
        PRIMARY KEY (user_id, payment_id)
    );
    CREATE TABLE IF NOT EXISTS bot_config (
        key TEXT PRIMARY KEY,
        value TEXT
    );
''')
conn.commit()

# ── Migração ────────────────────────────────────────────────────────────────

imported = updated = skipped = errors = 0

for user_id, info in data.items():

    # Chave especial de config
    if user_id == "config_bot_parado":
        value = 'true' if info else 'false'
        conn.execute('INSERT OR REPLACE INTO bot_config (key,value) VALUES (?,?)',
                     ('bot_parado', value))
        conn.commit()
        print(f"  [CONFIG] bot_parado = {value}")
        continue

    if not isinstance(info, dict):
        print(f"  [SKIP] {user_id} — valor nao e dicionario: {type(info)}")
        skipped += 1
        continue

    try:
        existing = conn.execute('SELECT user_id FROM bot_payments WHERE user_id=?',
                                (user_id,)).fetchone()

        record = {
            "status":           info.get("status", "novo"),
            "start_date":       info.get("start_date"),
            "end_date":         info.get("end_date"),
            "invite_link_url":  info.get("invite_link_url"),
            "last_payment_id":  info.get("last_payment_id"),
            "teste_usado":      1 if info.get("teste_usado") else 0,
            "ja_pagou_vip":     1 if info.get("ja_pagou_vip") else 0,
            "renewal_notified": 1 if info.get("renewal_notified") else 0,
            "last_pix_msg_id":  info.get("last_pix_msg_id"),
            "origem":           info.get("origem", "bot"),
            "created_at":       datetime.utcnow().isoformat(),
        }

        if existing:
            conn.execute('''
                UPDATE bot_payments SET
                    status=?, start_date=?, end_date=?, invite_link_url=?,
                    last_payment_id=?, teste_usado=?, ja_pagou_vip=?,
                    renewal_notified=?, last_pix_msg_id=?, origem=?
                WHERE user_id=?
            ''', (record["status"], record["start_date"], record["end_date"],
                  record["invite_link_url"], record["last_payment_id"],
                  record["teste_usado"], record["ja_pagou_vip"],
                  record["renewal_notified"], record["last_pix_msg_id"],
                  record["origem"], user_id))
            updated += 1
            action = "ATUALIZADO"
        else:
            conn.execute('''
                INSERT INTO bot_payments
                    (user_id, status, start_date, end_date, invite_link_url,
                     last_payment_id, teste_usado, ja_pagou_vip, renewal_notified,
                     last_pix_msg_id, origem, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (user_id, record["status"], record["start_date"], record["end_date"],
                  record["invite_link_url"], record["last_payment_id"],
                  record["teste_usado"], record["ja_pagou_vip"],
                  record["renewal_notified"], record["last_pix_msg_id"],
                  record["origem"], record["created_at"]))
            imported += 1
            action = "IMPORTADO"

        # processed_payments
        for pid in info.get("processed_payments", []):
            try:
                conn.execute('''
                    INSERT OR IGNORE INTO bot_processed_payments (user_id, payment_id, processed_at)
                    VALUES (?, ?, ?)
                ''', (user_id, pid, datetime.utcnow().isoformat()))
            except Exception:
                pass

        status_str = info.get("status", "novo").upper()
        end_str    = info.get("end_date", "")[:10] if info.get("end_date") else "—"
        print(f"  [{action}] ID {user_id:>12} | status={status_str:<12} | expira={end_str}")

        conn.commit()

    except Exception as e:
        print(f"  [ERRO] ID {user_id}: {e}")
        errors += 1

conn.close()

print(f"\n{'='*55}")
print(f"  Migração concluída!")
print(f"  Importados : {imported}")
print(f"  Atualizados: {updated}")
print(f"  Ignorados  : {skipped}")
print(f"  Erros      : {errors}")
print(f"{'='*55}")
print()
print("  Próximos passos:")
print("  1. Copie o bazuka.db gerado para o Railway (via volume ou upload)")
print("  2. Configure a variável SITE_PUBLIC_URL no Railway")
print("  3. Faça o deploy do main.py")
print()
