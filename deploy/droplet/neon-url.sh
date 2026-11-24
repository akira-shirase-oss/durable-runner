# shellcheck shell=bash
# Sourced by neon-setup.sh. Copied from Colophon's deploy/droplet/neon-url.sh (same Neon, same
# reasons), with the URL split added so psql never sees a password on its command line.
#
# Neon's dashboard hands out the POOLED host with ?sslmode=require&channel_binding=require.
# Durable Runner needs the DIRECT host (sqlx's prepared statements and the migrator's session
# advisory lock don't survive a transaction pooler) and verify-full (require encrypts without
# checking who answers; the account store refuses anything else for a non-loopback host).

# neon_url_fix <url>: prints the fixed URL; NEON_URL_NOTES lists what changed.
neon_url_fix() {
  local url=$1
  NEON_URL_NOTES=()
  if [[ "$url" =~ ^(postgres(ql)?://[^@]+@[^/.]+)-pooler(\.[^/]+/.*)$ ]]; then
    url="${BASH_REMATCH[1]}${BASH_REMATCH[3]}"
    NEON_URL_NOTES+=("the -pooler host became the direct one")
  fi
  if [[ "$url" == *channel_binding=* ]]; then
    url=$(sed -E 's/([?&])channel_binding=[^&]*&?/\1/; s/[?&]$//' <<<"$url")
    NEON_URL_NOTES+=("channel_binding dropped")
  fi
  if [[ "$url" == *sslmode=* ]]; then
    if [[ "$url" != *sslmode=verify-full* ]]; then
      url=$(sed -E 's/sslmode=[a-z-]+/sslmode=verify-full/' <<<"$url")
      NEON_URL_NOTES+=("sslmode set to verify-full")
    fi
  else
    url="$url$([[ "$url" == *\?* ]] && echo '&' || echo '?')sslmode=verify-full"
    NEON_URL_NOTES+=("sslmode=verify-full added")
  fi
  printf '%s' "$url"
}

# neon_url_split <url>: sets NEON_USER NEON_PASSWORD (percent-decoded) NEON_HOST NEON_PORT
# NEON_DB NEON_QUERY from postgres://user:password@host[:port]/db[?query], or returns 1.
neon_url_split() {
  [[ "$1" =~ ^postgres(ql)?://([^:@/]+):([^@/]+)@([^:/?]+)(:([0-9]+))?/([^?]+)(\?(.*))?$ ]] || return 1
  NEON_USER=${BASH_REMATCH[2]} NEON_PASSWORD=${BASH_REMATCH[3]} NEON_HOST=${BASH_REMATCH[4]}
  NEON_PORT=${BASH_REMATCH[6]:-5432} NEON_DB=${BASH_REMATCH[7]} NEON_QUERY=${BASH_REMATCH[9]:-}
  NEON_PASSWORD=$(printf '%b' "${NEON_PASSWORD//%/\\x}")
}
