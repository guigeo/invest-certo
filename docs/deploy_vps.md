# Deploy em VPS

Este guia descreve o caminho operacional recomendado para rodar o Invest Certo diariamente em uma VPS Ubuntu.

Para o estado real atual da VPS ja publicada, consulte tambem `docs/vps_infra_current.md`.

## Requisitos

- Ubuntu 24.04 LTS
- usuario dedicado `investcerto`
- `git`
- `curl`
- `nginx`
- `certbot`
- `uv`
- repositorio em `/opt/invest-certo`

## Preparar servidor

```bash
sudo adduser --system --group --home /opt/invest-certo investcerto
sudo apt update
sudo apt install -y git curl nginx apache2-utils certbot python3-certbot-nginx
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Garanta que `uv` esteja disponivel para os servicos. Os templates `systemd` procuram em `/opt/invest-certo/.local/bin`, `/usr/local/bin`, `/usr/bin` e `/bin`.

Clone ou copie o repositorio para `/opt/invest-certo` e ajuste permissao:

```bash
sudo chown -R investcerto:investcerto /opt/invest-certo
```

## Configurar variaveis

Crie `.env` a partir de `.env.example`:

```bash
cp .env.example .env
```

Preencha `TELEGRAM_TOKEN` e `CHAT_ID` se quiser alertas. Se ficarem vazios, o pipeline roda sem alerta.

## Sincronizar dependencias

```bash
UV_CACHE_DIR=.uv-cache uv sync --extra dev
```

## Testar manualmente

```bash
./run_pipeline.sh
UV_CACHE_DIR=.uv-cache uv run python -m pytest
```

## Instalar systemd

Copie os arquivos:

```bash
sudo cp ops/systemd/invest-certo-pipeline.service /etc/systemd/system/
sudo cp ops/systemd/invest-certo-pipeline.timer /etc/systemd/system/
sudo cp ops/systemd/invest-certo-dashboard.service /etc/systemd/system/
sudo systemctl daemon-reload
```

Ative pipeline diario e dashboard:

```bash
sudo systemctl enable --now invest-certo-pipeline.timer
sudo systemctl enable --now invest-certo-dashboard.service
```

O timer do pipeline usa `Tue..Sat 00:00:00` em UTC, equivalente a `Mon..Fri 21:00` no horario de Brasilia.

## Atualizar codigo na VPS

Depois do primeiro deploy, o fluxo recomendado e:

```bash
cd /opt/invest-certo
scripts/deploy_pull.sh
```

Esse script executa:

- `git pull --ff-only`
- `uv sync`
- restart do dashboard

Para tambem disparar o pipeline logo apos baixar uma nova versao:

```bash
scripts/deploy_pull.sh --run-pipeline
```

Para sincronizar dependencias de desenvolvimento e rodar testes na VPS:

```bash
scripts/deploy_pull.sh --test
```

Comandos uteis:

```bash
systemctl list-timers invest-certo-pipeline.timer
journalctl -u invest-certo-pipeline.service -n 200
journalctl -u invest-certo-dashboard.service -f
```

## Dashboard

O dashboard escuta em `127.0.0.1:8501`. Para expor na internet, use Nginx como proxy reverso com HTTPS e protecao de acesso.

Antes, aponte um dominio ou subdominio para o IP da VPS. Exemplo:

```text
dash.seudominio.com -> IP_DA_VPS
```

Copie o template de Nginx e ajuste `server_name`:

```bash
sudo cp ops/nginx/invest-certo.conf /etc/nginx/sites-available/invest-certo.conf
sudo nano /etc/nginx/sites-available/invest-certo.conf
```

Crie usuario e senha para Basic Auth:

```bash
sudo htpasswd -c /etc/nginx/.htpasswd-invest-certo seu_usuario
```

Ative o site:

```bash
sudo ln -s /etc/nginx/sites-available/invest-certo.conf /etc/nginx/sites-enabled/invest-certo.conf
sudo nginx -t
sudo systemctl reload nginx
```

Emita HTTPS com Certbot:

```bash
sudo certbot --nginx -d dash.seudominio.com
```

Depois disso, o acesso esperado e:

```text
https://dash.seudominio.com
```

O dashboard deve pedir usuario e senha antes de carregar o Streamlit.

## Dashboard com Caddy e Cloudflare

No deploy atual da VPS, o dashboard tambem pode ser exposto com Caddy como reverse proxy e DNS na Cloudflare.

Estado atual usado em producao:

```text
invest-certo-dash.averisen.com -> 91.99.176.140
Caddy -> 127.0.0.1:8501
Streamlit -> 127.0.0.1:8501
```

O registro DNS inicial deve ficar como `DNS only` na Cloudflare ate o Caddy emitir o certificado HTTPS.

Exemplo de `/etc/caddy/Caddyfile`:

```caddyfile
invest-certo-dash.averisen.com {
    reverse_proxy 127.0.0.1:8501
}
```

Valide e recarregue:

```bash
caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
```

Depois que `https://invest-certo-dash.averisen.com` responder corretamente, ative o proxy da Cloudflare e configure SSL/TLS como `Full (strict)`.

## Backup

O backup de `data/` e `logs/` fica fora deste pacote de deploy. Confirme a rotina existente antes de depender da VPS como ambiente principal.
