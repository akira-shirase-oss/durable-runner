# shellcheck shell=bash disable=SC2034  # a library: its settings are read by the scripts that source it
# Sourced by the shared-droplet scripts (install.sh, caddy-merge.sh, smoke.sh, put-secret.sh,
# direct/droplet-deploy.sh). The droplet is shared: Colophon already serves from it, so every
# path below is Durable Runner's own, and nothing here writes outside them except the one import
# line caddy-merge.sh adds to the root Caddyfile.
#
# ROOT is a prefix every path honours: empty on the droplet, a scratch directory in the tests
# (test-droplet-install.sh, test-droplet-deploy.sh). Paths written INTO configuration (the unit,
# the Caddy import line) are the droplet's own, without ROOT.

ROOT=${ROOT:-}
DR_ETC=$ROOT/etc/durable
DR_CREDS=$DR_ETC/credentials
DR_ENV=$DR_ETC/durable.env
DR_STATE=$ROOT/var/lib/durable
DR_OPT=$ROOT/opt/durable
# The binaries and the start-up scripts sit where the self-hosted install puts them, so
# deploy/upgrade.sh (its staged copy, durable-server.prev, restore_prev) and deploy/rollback.sh
# work here unchanged: /usr/local/bin/durable-server (+ .prev), durable-cli, durable-run,
# durable-lab-guard, and this kit's durable-saas-run. Only the dashboard is versioned by release
# directory: /opt/durable/dashboards/<release>, served through the /opt/durable/dashboard link.
DR_BIN=$ROOT/usr/local/bin
DR_DASHES=$DR_OPT/dashboards
DR_DASH_LINK=$DR_OPT/dashboard
DR_BUILD=${DURABLE_BUILD_DIR:-$ROOT/opt/durable-build}
DR_UNIT_FILE=$ROOT/etc/systemd/system/durable-server.service
DR_UNIT=durable-server
DR_USER=durable
CADDY_DIR=$ROOT/etc/caddy
# The first line of every file this kit owns outside its own directories: an installer refuses
# to overwrite a file of that name that doesn't carry it (it isn't ours).
DR_MARK='# Durable Runner, shared droplet (deploy/droplet)'
# Durable Runner's loopback ports on the shared host (Colophon has 58000): the public listener Caddy proxies to, and the ops
# listener (--ops-listen) reached only through an SSH tunnel. The tests move them.
DR_PUBLIC_PORT=${DURABLE_PUBLIC_PORT:-58300}
DR_OPS_PORT=${DURABLE_OPS_PORT:-58301}
DR_SITE_HOST_SHIPPED=durablerunnerapp.404human.dev
DR_DASHBOARD_SHIPPED=/app/

die() { echo "error: $*" >&2; exit 1; }
say() { printf '   %s\n' "$*"; }

# Values from the environment or a file meet shell arithmetic only after this (bash evaluates
# array subscripts there: x[$(cmd)] in (( )) runs cmd).
is_uint() { [[ "${1:-}" =~ ^[0-9]+$ ]]; }
for _p in "$DR_PUBLIC_PORT" "$DR_OPS_PORT"; do
  if ! is_uint "$_p" || (( _p == 0 || _p > 65535 )); then die "DURABLE_PUBLIC_PORT / DURABLE_OPS_PORT: '$_p' isn't a port"; fi
done
unset _p

# mode_of <path>: the octal permission bits (GNU stat on the droplet, BSD stat in a Mac test).
mode_of() { stat -c %a "$1" 2>/dev/null || stat -f %Lp "$1"; }

# sha256_of <file>: the hex digest alone.
sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1; else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

# put_file <src> <dst> <mode>: installs src at dst (written whole, then renamed) unless dst already
# has the same bytes and mode. Returns 0 when it changed something, 1 when dst was already right.
put_file() {
  local src=$1 dst=$2 mode=$3 tmp
  if [[ -f "$dst" && ! -L "$dst" ]] && cmp -s "$src" "$dst" && [[ "$(mode_of "$dst")" == "${mode#0}" ]]; then
    return 1
  fi
  tmp=$(mktemp "$(dirname "$dst")/.dr-put.XXXXXX")
  cat "$src" >"$tmp"
  chmod "$mode" "$tmp"
  mv -f "$tmp" "$dst"
  return 0
}

# ensure_dir <path> <mode> [owner]: creates it, or fixes its mode; the owner (user:group) only on
# the droplet (a test runs unprivileged; its fake chown records the call).
ensure_dir() {
  local dir=$1 mode=$2 owner=${3:-}
  if [[ ! -d "$dir" ]]; then
    mkdir -p "$dir"
    chmod "$mode" "$dir"
    [[ -z "$owner" ]] || chown "$owner" "$dir"
    say "made $dir"
  elif [[ "$(mode_of "$dir")" != "${mode#0}" ]]; then
    chmod "$mode" "$dir"
    say "mode $mode on $dir"
  fi
}

# env_get <key> <file>: the file's last KEY=value, one pair of surrounding double quotes removed
# (systemd's EnvironmentFile takes them off too). Prints nothing when the key is absent.
env_get() {
  local v
  v=$(sed -n "s/^$1=//p" "$2" 2>/dev/null | tail -1 | tr -d '\r')
  v=${v#\"}; v=${v%\"}
  printf '%s' "$v"
}

# host_valid <host>: a DNS name of at least two labels, each 1-63 of [a-z0-9-] not starting or
# ending with '-' (as Colophon's public-host.sh).
host_valid() {
  local host=$1 label='[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?'
  ((${#host} <= 253)) && [[ "$host" =~ ^($label\.)+$label$ ]]
}

# origin_host <origin>: the host of https://<host> (no port, path, query or credentials), or fails.
origin_host() {
  local origin=${1%/} host
  [[ "$origin" == https://* ]] || { echo "error: the public origin must be https://<host>, not '$1'" >&2; return 1; }
  host=${origin#https://}
  host_valid "$host" || { echo "error: '$host' isn't a plain lowercase hostname (no port, path or credentials)" >&2; return 1; }
  printf '%s' "$host"
}

# dashboard_path_valid <path>: /inspector/ or /app/ style: a leading and a trailing slash, nothing a
# Caddyfile or a URL would read as more.
dashboard_path_valid() { [[ "$1" =~ ^/([a-z0-9_-]+/)+$ ]]; }

# render_site <site-file> <host> <dashboard-path>: prints the shipped site file with its one site
# address line and its one root redirect replaced; fails if either line isn't there exactly once.
render_site() {
  local file=$1 host=$2 dash=$3 addr redir count l
  host_valid "$host" || { echo "error: '$host' isn't a plain lowercase hostname" >&2; return 1; }
  dashboard_path_valid "$dash" || { echo "error: the dashboard path '$dash' must look like /app/" >&2; return 1; }
  addr="$DR_SITE_HOST_SHIPPED:443 {"
  redir=$'\t'"redir / $DR_DASHBOARD_SHIPPED 302"
  for l in "$addr" "$redir"; do
    count=$(grep -cxF -- "$l" "$file" || true)
    [[ "$count" == 1 ]] || { echo "error: $file has $count lines '$l', wanted exactly 1" >&2; return 1; }
  done
  while IFS= read -r l || [[ -n "$l" ]]; do
    if [[ "$l" == "$addr" ]]; then printf '%s:443 {\n' "$host"
    elif [[ "$l" == "$redir" ]]; then printf '\tredir / %s 302\n' "$dash"
    else printf '%s\n' "$l"; fi
  done <"$file"
}

# unit_main_pid: durable-server's MainPID, 0 when it isn't running (or systemd can't say).
unit_main_pid() {
  local pid
  pid=$(systemctl show -p MainPID --value "$DR_UNIT" 2>/dev/null || true)
  is_uint "$pid" || pid=0
  printf '%s' "$pid"
}

# port_listeners <port>: every TCP listener on the port, any address, as "<pid> <command>" lines;
# "? ?" for one whose process this user can't see. Returns 2 when there is no way to ask.
port_listeners() {
  local port=$1 out line pid cmd
  if command -v ss >/dev/null 2>&1; then
    out=$(ss -Hltnp "sport = :$port" 2>/dev/null) || return 2
    while IFS= read -r line; do
      [[ -n "$line" ]] || continue
      if [[ "$line" =~ \(\(\"([^\"]*)\",pid=([0-9]+), ]]; then
        printf '%s %s\n' "${BASH_REMATCH[2]}" "${BASH_REMATCH[1]}"
      else
        printf '? ?\n'
      fi
    done <<<"$out"
  elif command -v lsof >/dev/null 2>&1; then
    # -F: one field per line, p<pid> then c<command> per process. lsof exits 1 when nothing listens.
    out=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -Fpc 2>/dev/null || true)
    pid=""
    while IFS= read -r line; do
      case "$line" in
        p*) pid=${line#p} ;;
        c*) cmd=${line#c}; printf '%s %s\n' "${pid:-?}" "$cmd"; pid="" ;;
      esac
    done <<<"$out"
  else
    return 2
  fi
}

# port_check <port> <what>: fails closed unless nothing listens on the port, or the one listening is
# durable-server itself (the unit's MainPID). Names the pid and command of anything else and leaves
# it running (a port held by another process is a guard, never a target).
port_check() {
  local port=$1 what=$2 listeners ours pid cmd foreign=""
  listeners=$(port_listeners "$port") || die "can't tell what listens on $port ($what): neither ss nor lsof is here; refusing"
  [[ -n "$listeners" ]] || return 0
  ours=$(unit_main_pid)
  while read -r pid cmd; do
    if [[ "$pid" != "?" && "$ours" != 0 && "$pid" == "$ours" ]]; then continue; fi
    foreign="$foreign pid $pid ($cmd);"
  done <<<"$listeners"
  [[ -z "$foreign" ]] || die "port $port ($what) is held by something that isn't durable-server:$foreign it was left running; free the port or change the band"
  return 0
}

# install_scripts <deploy-dir>: durable-run, durable-lab-guard and durable-saas-run from a deploy/
# tree into /usr/local/bin. Returns 0 when it changed any, 1 when all three were current.
install_scripts() {
  local d=$1 changed=1 pair
  for pair in durable-run.sh:durable-run lab-build-guard.sh:durable-lab-guard droplet/durable-saas-run.sh:durable-saas-run; do
    [[ -s "$d/${pair%%:*}" ]] || die "$d/${pair%%:*} is missing"
    if put_file "$d/${pair%%:*}" "$DR_BIN/${pair##*:}" 0755; then changed=0; fi
  done
  return "$changed"
}
