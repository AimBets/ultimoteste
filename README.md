# BazukaBets — Sistema Unificado

## Estrutura de arquivos

```
BAZUKA_FINAL/
├── main.py                ← Código único: site + bot + pagamentos
├── requirements.txt
├── Procfile               ← Para Railway (gunicorn)
├── migrate_payments.py    ← Script de importação do payments.json legado
└── templates/
    ├── index.html         ← Site original (copie do projeto anterior)
    ├── apresentacao.html  ← Apresentação (copie do projeto anterior)
    └── vip_checkout.html  ← Página VIP nova (já incluída aqui)
```

---

## Variáveis de ambiente no Railway

Configure estas variáveis em **Settings → Variables**:

| Variável           | Descrição                                              | Exemplo                         |
|--------------------|--------------------------------------------------------|---------------------------------|
| `BOT_TOKEN`        | Token do bot Telegram                                  | `8363865808:AAF...`             |
| `MP_ACCESS_TOKEN`  | Token do Mercado Pago                                  | `APP_USR-679...`                |
| `SECRET_KEY`       | Chave secreta do Flask (troque por algo aleatório)     | `minha-chave-super-secreta-123` |
| `ADMIN_USER`       | Email do admin do site                                 | `bazukabets@gmail.com`          |
| `ADMIN_PASS`       | Senha do admin do site                                 | `SuaSenhaForte123`              |
| `SITE_SECRET_KEY`  | Chave compartilhada entre site e bot                   | `uB5G6Zfn8Ij7wJon...`          |
| `SITE_PUBLIC_URL`  | URL pública do Railway **sem barra final**             | `https://bazuka.railway.app`    |
| `DB_PATH`          | Caminho do banco SQLite (use volume persistente)       | `/data/bazuka.db`               |
| `SMTP_HOST`        | (Opcional) Servidor SMTP para emails                   | `smtp.gmail.com`                |
| `SMTP_PORT`        | (Opcional) Porta SMTP                                  | `587`                           |
| `SMTP_USER`        | (Opcional) Email SMTP                                  | `bazukabets@gmail.com`          |
| `SMTP_PASS`        | (Opcional) Senha SMTP ou App Password                  | `abcd efgh ijkl mnop`           |

---

## Persistência de dados no Railway

O Railway apaga o filesystem a cada deploy. Para os dados não se perderem:

### Opção A — Volume persistente (recomendado)
1. No Railway, vá em **Add New → Volume**
2. Monte em `/data`
3. Configure `DB_PATH=/data/bazuka.db`

### Opção B — PostgreSQL (alternativa)
Adicione o plugin PostgreSQL e adapte as queries (não incluído neste código).

---

## Migração do payments.json legado

**Execute uma vez antes do primeiro deploy:**

```bash
# No seu computador local, com o payments.json antigo
python migrate_payments.py payments.json bazuka.db

# Depois faça upload do bazuka.db para o volume /data do Railway
# Ou use a rota admin após o deploy:
# POST /api/admin/import_payments_json  (envie o arquivo como multipart/form-data)
```

---

## Deploy no Railway

1. Copie os arquivos `index.html` e `apresentacao.html` do projeto antigo para a pasta `templates/`
2. Se tiver pasta `static/` (imagens, etc.), copie também
3. Suba tudo para um repositório Git
4. No Railway: **New Project → Deploy from GitHub repo**
5. Configure as variáveis de ambiente
6. O Railway vai usar o `Procfile` automaticamente

---

## Fluxos disponíveis

### Via Bot Telegram
- `/start` → menu com teste gratuito + VIP via PIX + link para o site
- `/apostabrasil` → teste de 4 dias (especial)
- `/reembolso` → solicitar estorno
- `/vips`, `/comunicado`, `/imagem` → admin: comunicados em massa
- `/stop`, `/voltar` → admin: pausar/reativar vendas

### Via Site
- `/vip` → página de checkout com visual do site
- Pagamento PIX → código `BBV-XXXXXXXX` gerado
- Usuário cola o código no bot → acesso liberado

### Webhooks
- `POST /telegram_webhook` → recebe updates do Telegram (produção)
- `POST /mercadopago_webhook` → pagamentos via bot
- `POST /api/vip/webhook` → pagamentos via site

---

## Novos endpoints admin

| Método | Rota                              | Descrição                          |
|--------|-----------------------------------|------------------------------------|
| POST   | `/api/admin/import_payments_json` | Importa payments.json pelo browser |
| GET    | `/api/admin/vip_payments`         | Lista pagamentos VIP via site      |

---

## Diferenças em relação ao sistema anterior

| Antes                          | Agora                              |
|--------------------------------|------------------------------------|
| `payments.json` (apagado no Railway) | Tabela `bot_payments` no SQLite ✅ |
| `vip_codes.json`              | Tabela `vip_codes` no SQLite ✅    |
| Bot e site em arquivos separados | Um único `main.py` ✅             |
| Página VIP genérica           | Visual integrado ao site ✅        |
| Bot notifica via HTTP interno | Bot e site no mesmo processo ✅    |
