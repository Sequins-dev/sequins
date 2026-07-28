#!/usr/bin/env bash
#
# Launch OpenAI Codex CLI pointed at the Sequins assistant's **Responses API**
# endpoint (`/v1/responses`).
#
# Codex speaks the Responses API, so — unlike the Chat Completions path — it
# receives the assistant's server-executed tool activity as typed output items.
# This is the client to use to check the `/v1/responses` surface end-to-end.
#
# Usage:
#   scripts/codex-assistant.sh "how many spans are there in the last hour?"
#   scripts/codex-assistant.sh                 # opens the interactive TUI
#
# Environment overrides:
#   RESPONSES_URL     Base URL (default http://127.0.0.1:8082/v1; Codex POSTs /responses under it)
#   MODEL_ID          Model id to use (default: first advertised by /v1/models)
#   ASSISTANT_BEARER  Bearer token sent to the endpoint (default "local"). Keyless
#                     daemons ignore it; must match a --api-key if the daemon is keyed.
set -euo pipefail

RESPONSES_URL="${RESPONSES_URL:-http://127.0.0.1:8082/v1}"
PROVIDER_ID="sequins"
ASSISTANT_BEARER="${ASSISTANT_BEARER:-local}"

# --- 1. Codex installed? ------------------------------------------------------
if ! command -v codex >/dev/null 2>&1; then
  cat <<'EOF' >&2
codex not found. Install it, then re-run:
  npm i -g @openai/codex          # or: brew install codex
Docs: https://developers.openai.com/codex/cli
EOF
  exit 1
fi

# --- 2. Endpoint reachable? ---------------------------------------------------
echo "Checking assistant at ${RESPONSES_URL}/models ..." >&2
if ! models_json="$(curl -fsS -H "Authorization: Bearer ${ASSISTANT_BEARER}" "${RESPONSES_URL}/models" 2>/dev/null)"; then
  cat <<EOF >&2
Could not reach the assistant at ${RESPONSES_URL}.

Start the Sequins Pro daemon (auto-enables the assistant when OPENAI_API_KEY is set):

  cd ../sequins-pro
  export OPENAI_API_KEY=sk-... OPENAI_MODEL=gpt-4o-mini
  cargo run -p sequins-pro-daemon --bin sequins-daemon -- start \\
      --config sequins-storage.yaml --assistant-addr 0.0.0.0:8082
EOF
  exit 1
fi

# --- 3. Resolve a model id ----------------------------------------------------
if [[ -z "${MODEL_ID:-}" ]]; then
  if command -v jq >/dev/null 2>&1; then
    MODEL_ID="$(printf '%s' "$models_json" | jq -r '.data[0].id // empty')"
  else
    MODEL_ID="$(printf '%s' "$models_json" | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
  fi
fi
if [[ -z "${MODEL_ID:-}" ]]; then
  echo "The assistant advertises no models (/v1/models is empty). Configure a server-side model" >&2
  echo "(e.g. set OPENAI_API_KEY on the daemon) so Codex has one to select." >&2
  exit 1
fi
echo "Using model: ${MODEL_ID} via ${PROVIDER_ID} (wire_api=responses)" >&2

# --- 4. Isolated Codex config pointing at our Responses endpoint ---------------
# A private CODEX_HOME so we never touch the user's ~/.codex config. The key is
# read from an env var (Codex requires one for a custom provider); keyless daemons
# ignore it.
CODEX_HOME="$(mktemp -d)"
export CODEX_HOME
trap 'rm -rf "$CODEX_HOME"' EXIT
export SEQUINS_ASSISTANT_KEY="$ASSISTANT_BEARER"

cat > "${CODEX_HOME}/config.toml" <<EOF
model = "${MODEL_ID}"
model_provider = "${PROVIDER_ID}"
approval_policy = "never"

[model_providers.${PROVIDER_ID}]
name = "Sequins Assistant"
base_url = "${RESPONSES_URL}"
env_key = "SEQUINS_ASSISTANT_KEY"
wire_api = "responses"
EOF

echo "Codex config at ${CODEX_HOME}/config.toml; launching ..." >&2

# With a prompt → non-interactive `codex exec`; without → interactive TUI.
if [[ "$#" -gt 0 ]]; then
  exec codex exec --skip-git-repo-check -s read-only "$@"
else
  exec codex --skip-git-repo-check -s read-only
fi
