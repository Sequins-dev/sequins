#!/usr/bin/env bash
#
# Launch OpenCode pointed at the Sequins assistant as its model provider.
#
# The Sequins Pro daemon exposes an OpenAI-compatible endpoint (/v1) whose model
# is a tool-injecting agent over your telemetry. Connecting OpenCode to it means
# OpenCode's own tools AND the assistant's in-server data tools are both available
# to the model — the "proxy" mode. Ask it a question about your data and watch it
# call run_sql / column_profile / run_seql server-side.
#
# Usage:
#   scripts/opencode-assistant.sh [extra opencode args...]
#
# Environment overrides:
#   ASSISTANT_URL     Base URL of the assistant (default http://127.0.0.1:8082/v1)
#   MODEL_ID          Model id to use (default: first advertised by /v1/models)
#   ASSISTANT_BEARER  Bearer token sent to the endpoint (default "local"). Keyless
#                     daemons ignore it; if the daemon was started with --api-key,
#                     this must match one of those keys.
set -euo pipefail

ASSISTANT_URL="${ASSISTANT_URL:-http://127.0.0.1:8082/v1}"
PROVIDER_ID="sequins"
ASSISTANT_BEARER="${ASSISTANT_BEARER:-local}"

# --- 1. OpenCode installed? ---------------------------------------------------
if ! command -v opencode >/dev/null 2>&1; then
  cat <<'EOF' >&2
opencode not found. Install it, then re-run this script:
  curl -fsSL https://opencode.ai/install | bash     # or: npm i -g opencode-ai
Docs: https://opencode.ai/docs/
EOF
  exit 1
fi

# --- 2. Endpoint reachable? ---------------------------------------------------
echo "Checking assistant at ${ASSISTANT_URL}/models ..." >&2
if ! models_json="$(curl -fsS -H "Authorization: Bearer ${ASSISTANT_BEARER}" "${ASSISTANT_URL}/models" 2>/dev/null)"; then
  cat <<EOF >&2
Could not reach the assistant at ${ASSISTANT_URL}.

Start the Sequins Pro daemon with the assistant enabled (it auto-enables when
OPENAI_API_KEY is set — the daemon is meant to be the only process in its container):

  cd ../sequins-pro
  export OPENAI_API_KEY=sk-...             # your provider key (or a dummy for a local model)
  # Optional: target a local OpenAI-compatible model instead of api.openai.com
  # export OPENAI_BASE_URL=http://localhost:11434/v1
  # export OPENAI_MODEL=llama3.1
  cargo run -p sequins-pro-daemon --bin sequins-daemon -- start \\
      --config sequins-storage.yaml --assistant-addr 0.0.0.0:8082
EOF
  exit 1
fi

# --- 3. Resolve a model id ----------------------------------------------------
# Prefer an explicit MODEL_ID; otherwise take the first model /v1/models advertises.
if [[ -z "${MODEL_ID:-}" ]]; then
  if command -v jq >/dev/null 2>&1; then
    MODEL_ID="$(printf '%s' "$models_json" | jq -r '.data[0].id // empty')"
  else
    # Minimal fallback parse of the first "id":"..." without jq.
    MODEL_ID="$(printf '%s' "$models_json" | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
  fi
fi

if [[ -z "${MODEL_ID:-}" ]]; then
  cat <<EOF >&2
The assistant is running but advertises no models (/v1/models is empty), so OpenCode
has nothing to select. Configure a server-side model — the simplest way is to set
OPENAI_API_KEY (and optionally OPENAI_BASE_URL / OPENAI_MODEL) on the daemon and restart.
EOF
  exit 1
fi

echo "Using model: ${PROVIDER_ID}/${MODEL_ID}" >&2

# --- 4. Generate an OpenCode config and launch --------------------------------
# OpenCode reads opencode.json from the working directory; use a scratch dir so we
# don't touch your project. Pass through any extra args (e.g. a prompt, --model).
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
cat > "${workdir}/opencode.json" <<EOF
{
  "\$schema": "https://opencode.ai/config.json",
  "provider": {
    "${PROVIDER_ID}": {
      "npm": "@ai-sdk/openai-compatible",
      "name": "Sequins Assistant",
      "options": {
        "baseURL": "${ASSISTANT_URL}",
        "apiKey": "${ASSISTANT_BEARER}"
      },
      "models": {
        "${MODEL_ID}": {
          "name": "Sequins ${MODEL_ID}",
          "limit": { "context": 128000, "output": 8192 }
        }
      }
    }
  },
  "model": "${PROVIDER_ID}/${MODEL_ID}"
}
EOF

echo "Wrote ${workdir}/opencode.json; launching opencode ..." >&2
cd "$workdir"
exec opencode "$@"
