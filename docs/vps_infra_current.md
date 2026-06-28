# Infra atual da VPS

Este arquivo registra o estado real da publicacao do dashboard para retomada futura. Nao registrar chaves privadas, tokens, senhas ou conteudo sensivel aqui.

## Acesso

- Alias SSH local para usuario operacional: `ssh hetzner-gramos`
- Alias SSH local para administracao: `ssh hetzner`
- A configuracao completa dos aliases fica fora do repositorio, no ambiente local do usuario.
- Antes de pedir dados de acesso novamente, consultar a configuracao SSH local.

## Servidor

- Host observado: `homelablinux`
- IP publico usado no DNS: `91.99.176.140`
- Sistema observado: Ubuntu 26.04 LTS
- Projeto na VPS: `/home/gramos/projects/invest-certo`
- O deploy atual nao usa Docker para a aplicacao; `docker ps` estava sem containers ativos durante a configuracao.

## Aplicacao

- Dashboard: Streamlit
- Execucao: systemd de usuario
- Unit: `/home/gramos/.config/systemd/user/invest-certo-dashboard.service`
- Timer do pipeline: `/home/gramos/.config/systemd/user/invest-certo-pipeline.timer`
- Pipeline service: `/home/gramos/.config/systemd/user/invest-certo-pipeline.service`
- App interno: `127.0.0.1:8501`
- Comando do dashboard na unit:

```bash
/home/gramos/.local/bin/uv run streamlit run app/streamlit_app.py --server.address 127.0.0.1 --server.port 8501
```

## Publicacao

- URL publica: `https://invest-certo-dash.averisen.com`
- DNS: Cloudflare
- Registro DNS:

```text
A invest-certo-dash -> 91.99.176.140
Proxy: Proxied
TTL: Auto
```

- SSL/TLS Cloudflare: `Full (strict)`
- Reverse proxy na VPS: Caddy
- Caddyfile ativo:

```caddyfile
invest-certo-dash.averisen.com {
    reverse_proxy 127.0.0.1:8501
}
```

- Caminho do Caddyfile: `/etc/caddy/Caddyfile`
- Backup criado durante a configuracao: `/etc/caddy/Caddyfile.bak.20260628152201`

## Firewall e portas

- UFW esta ativo.
- Politica observada apos configuracao:
  - entrada: deny
  - saida: allow
- Portas liberadas:
  - `22/tcp` para SSH
  - `80/tcp` para HTTP/redirect e desafios
  - `443/tcp` para HTTPS
- Streamlit nao deve escutar em interface publica; manter em `127.0.0.1:8501`.

## Validacoes feitas

- HTTP redirecionou para HTTPS:

```text
http://invest-certo-dash.averisen.com -> 308 Permanent Redirect
```

- HTTPS respondeu o app:

```text
https://invest-certo-dash.averisen.com -> 200 OK
```

- Caddy obteve certificado automatico via Let's Encrypt para `invest-certo-dash.averisen.com`.

## Comandos uteis

Checar dashboard:

```bash
ssh hetzner-gramos 'systemctl --user status invest-certo-dashboard.service --no-pager'
```

Checar pipeline timer:

```bash
ssh hetzner-gramos 'systemctl --user status invest-certo-pipeline.timer --no-pager'
```

Checar Caddy:

```bash
ssh hetzner 'systemctl status caddy --no-pager'
```

Validar Caddyfile:

```bash
ssh hetzner 'caddy validate --config /etc/caddy/Caddyfile'
```

Ver portas expostas:

```bash
ssh hetzner 'ss -tulpn | grep -E ":(22|80|443|8501)\b" || true'
```

Testar endpoint publico:

```bash
curl -I https://invest-certo-dash.averisen.com
```

## Cuidados

- Nao remover containers, units, arquivos ou configuracoes existentes sem confirmar.
- Nao expor `8501` publicamente.
- Nao trocar Cloudflare para modo `Flexible`.
- Se o app precisar ser privado no futuro, preferir Cloudflare Access ou `basic_auth` no Caddy.
- Como a intencao atual e vitrine publica, revisar periodicamente se o dashboard nao revela segredos, `.env`, logs sensiveis ou dados privados.
