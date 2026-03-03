#!/bin/sh
# LynxDB Installer
# https://lynxdb.org/install.sh
#
# Usage:
#   curl -fsSL https://lynxdb.org/install.sh | sh
#   curl -fsSL https://lynxdb.org/install.sh | sh -s -- --version v0.5.0
#   curl -fsSL https://lynxdb.org/install.sh | LYNXDB_INSTALL_DIR=/opt/lynxdb sh
#
# Environment variables:
#   LYNXDB_INSTALL_DIR  - Installation directory (default: /usr/local/bin or ~/.local/bin)
#   LYNXDB_VERSION      - Specific version to install (default: latest)
#   LYNXDB_BASE_URL     - Override download base URL
#   LYNXDB_NO_MODIFY_PATH - If set, skip PATH modification
#
# This script is designed to be:
#   - POSIX sh compatible (no bash-isms)
#   - Simple and auditable
#   - Safe to pipe from curl

set -eu

# ─── Constants ────────────────────────────────────────────────────────────────

GITHUB_REPO="OrlovEvgeny/Lynxdb"
DEFAULT_BASE_URL="https://dl.lynxdb.org"
FALLBACK_BASE_URL="https://github.com/${GITHUB_REPO}/releases/download"
MANIFEST_URL="https://dl.lynxdb.org/manifest.json"

# ─── Terminal Colors (with graceful degradation) ──────────────────────────────

setup_colors() {
    if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
        BOLD="$(tput bold 2>/dev/null || printf '')"
        DIM="$(tput dim 2>/dev/null || printf '')"
        UNDERLINE="$(tput smul 2>/dev/null || printf '')"
        RED="$(tput setaf 1 2>/dev/null || printf '')"
        GREEN="$(tput setaf 2 2>/dev/null || printf '')"
        YELLOW="$(tput setaf 3 2>/dev/null || printf '')"
        BLUE="$(tput setaf 4 2>/dev/null || printf '')"
        MAGENTA="$(tput setaf 5 2>/dev/null || printf '')"
        CYAN="$(tput setaf 6 2>/dev/null || printf '')"
        RESET="$(tput sgr0 2>/dev/null || printf '')"
    else
        BOLD="" DIM="" UNDERLINE=""
        RED="" GREEN="" YELLOW="" BLUE="" MAGENTA="" CYAN="" RESET=""
    fi
}

# ─── Logging ──────────────────────────────────────────────────────────────────

info()      { printf '%s\n' "${BOLD}${CYAN}▸${RESET} $*"; }
success()   { printf '%s\n' "${GREEN}✔${RESET} $*"; }
warn()      { printf '%s\n' "${YELLOW}!${RESET} $*" >&2; }
error()     { printf '%s\n' "${RED}✖${RESET} $*" >&2; }
debug()     { [ -z "${VERBOSE:-}" ] || printf '%s\n' "${DIM}  $*${RESET}"; }

# ─── Utilities ────────────────────────────────────────────────────────────────

has() { command -v "$1" >/dev/null 2>&1; }

need() {
    if ! has "$1"; then
        error "Required command '$1' not found."
        error "Please install '$1' and try again."
        exit 1
    fi
}

# Detect available HTTP client
detect_http_client() {
    if has curl; then
        HTTP_CLIENT="curl"
    elif has wget; then
        HTTP_CLIENT="wget"
    else
        error "Neither 'curl' nor 'wget' found."
        error "Please install one of them and try again."
        exit 1
    fi
    debug "HTTP client: $HTTP_CLIENT"
}

# Generic HTTP GET to stdout
http_get() {
    url="$1"
    case "$HTTP_CLIENT" in
        curl) curl -fsSL "$url" ;;
        wget) wget -qO- "$url" ;;
    esac
}

# HTTP GET to file with progress
http_download() {
    url="$1"
    dest="$2"
    case "$HTTP_CLIENT" in
        curl)
            if [ -t 1 ]; then
                curl -fSL --progress-bar -o "$dest" "$url"
            else
                curl -fsSL -o "$dest" "$url"
            fi
            ;;
        wget)
            if [ -t 1 ]; then
                wget --show-progress -qO "$dest" "$url" 2>&1
            else
                wget -qO "$dest" "$url"
            fi
            ;;
    esac
}

# ─── Platform Detection ──────────────────────────────────────────────────────

detect_os() {
    os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    case "$os" in
        linux*)   OS="linux" ;;
        darwin*)  OS="darwin" ;;
        freebsd*) OS="freebsd" ;;
        msys*|mingw*|cygwin*) OS="windows" ;;
        *)
            error "Unsupported operating system: $os"
            error "LynxDB supports Linux, macOS, FreeBSD, and Windows (via WSL/Git Bash)."
            exit 1
            ;;
    esac
    debug "Detected OS: $OS"
}

detect_arch() {
    arch="$(uname -m)"
    case "$arch" in
        x86_64|amd64)   ARCH="amd64" ;;
        aarch64|arm64)  ARCH="arm64" ;;
        armv7*|armhf)   ARCH="armv7" ;;
        *)
            error "Unsupported architecture: $arch"
            error "LynxDB supports amd64 (x86_64), arm64, and armv7."
            exit 1
            ;;
    esac

    # Double-check: uname can mis-report 32-bit OS on 64-bit hardware
    if [ "$ARCH" = "amd64" ] && has getconf; then
        if [ "$(getconf LONG_BIT 2>/dev/null)" = "32" ]; then
            error "32-bit x86 is not supported. Please use a 64-bit OS."
            exit 1
        fi
    fi

    debug "Detected architecture: $ARCH"
}

detect_libc() {
    LIBC=""
    if [ "$OS" = "linux" ]; then
        if has ldd; then
            case "$(ldd --version 2>&1 || true)" in
                *musl*) LIBC="musl" ;;
                *)      LIBC="gnu" ;;
            esac
        elif [ -f /etc/alpine-release ]; then
            LIBC="musl"
        else
            LIBC="gnu"
        fi
        debug "Detected libc: $LIBC"
    fi
}

# ─── Version Resolution ──────────────────────────────────────────────────────

resolve_version() {
    if [ -n "${VERSION:-}" ]; then
        # Normalize: ensure version starts with 'v'
        case "$VERSION" in
            v*) ;; # already has prefix
            *)  VERSION="v${VERSION}" ;;
        esac
        debug "Using specified version: $VERSION"
        return
    fi

    info "Resolving latest version..."

    # Try primary manifest first, then fallback
    manifest=""
    if manifest="$(http_get "$MANIFEST_URL" 2>/dev/null)"; then
        debug "Fetched manifest from primary CDN"
    fi

    if [ -n "$manifest" ]; then
        # Extract version from JSON without jq dependency
        # Manifest format: {"version":"v0.5.0","channel":"stable",...}
        VERSION="$(printf '%s' "$manifest" | sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)"
        if [ -n "$VERSION" ]; then
            debug "Latest version from manifest: $VERSION"
            return
        fi
    fi

    # Final fallback: GitHub API
    if has curl; then
        VERSION="$(curl -fsSL "https://api.github.com/repos/${GITHUB_REPO}/releases/latest" 2>/dev/null \
            | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)"
    elif has wget; then
        VERSION="$(wget -qO- "https://api.github.com/repos/${GITHUB_REPO}/releases/latest" 2>/dev/null \
            | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)"
    fi

    if [ -z "${VERSION:-}" ]; then
        error "Could not determine the latest version."
        error "Please specify a version manually: --version v0.5.0"
        exit 1
    fi

    debug "Latest version from GitHub API: $VERSION"
}

# ─── Installation Directory ───────────────────────────────────────────────────

resolve_install_dir() {
    # Explicit override
    if [ -n "${INSTALL_DIR:-}" ]; then
        BIN_DIR="$INSTALL_DIR"
        debug "Using custom install dir: $BIN_DIR"
        return
    fi

    # Default: /usr/local/bin if writable, else ~/.local/bin
    if [ -d "/usr/local/bin" ] && [ -w "/usr/local/bin" ]; then
        BIN_DIR="/usr/local/bin"
    elif [ "$(id -u)" = "0" ]; then
        BIN_DIR="/usr/local/bin"
    else
        BIN_DIR="$HOME/.local/bin"
    fi

    debug "Install directory: $BIN_DIR"
}

# ─── Download & Install ──────────────────────────────────────────────────────

build_download_url() {
    # Construct artifact name: lynxdb-{version}-{os}-{arch}[.musl].tar.gz
    artifact="lynxdb-${VERSION}-${OS}-${ARCH}"
    if [ "$LIBC" = "musl" ]; then
        artifact="${artifact}-musl"
    fi

    if [ "$OS" = "windows" ]; then
        artifact="${artifact}.zip"
    else
        artifact="${artifact}.tar.gz"
    fi

    # Try CDN first, then GitHub Releases as fallback
    DOWNLOAD_URL="${BASE_URL}/${VERSION}/${artifact}"
    FALLBACK_DOWNLOAD_URL="${FALLBACK_BASE_URL}/${VERSION}/${artifact}"
    CHECKSUM_URL="${BASE_URL}/${VERSION}/checksums.txt"
    FALLBACK_CHECKSUM_URL="${FALLBACK_BASE_URL}/${VERSION}/checksums.txt"

    debug "Download URL: $DOWNLOAD_URL"
    debug "Artifact: $artifact"
}

create_tmp_dir() {
    TMP_DIR="$(mktemp -d 2>/dev/null || mktemp -d -t lynxdb-install)"
    debug "Temp directory: $TMP_DIR"

    # Cleanup on exit
    # shellcheck disable=SC2064
    trap "rm -rf \"$TMP_DIR\"" EXIT INT TERM
}

download_and_verify() {
    archive_path="${TMP_DIR}/${artifact}"

    info "Downloading ${BOLD}lynxdb ${VERSION}${RESET} for ${OS}/${ARCH}..."

    # Try CDN first, then fallback to GitHub Releases
    if ! http_download "$DOWNLOAD_URL" "$archive_path" 2>/dev/null; then
        debug "CDN download failed, trying GitHub Releases..."
        if ! http_download "$FALLBACK_DOWNLOAD_URL" "$archive_path"; then
            error "Download failed."
            printf '\n' >&2
            error "This could mean:"
            error "  • Version ${VERSION} doesn't exist"
            error "  • No build for ${OS}/${ARCH}${LIBC:+ ($LIBC)}"
            error "  • Network connectivity issues"
            printf '\n' >&2
            error "Available builds: ${UNDERLINE}https://github.com/${GITHUB_REPO}/releases/${VERSION}${RESET}"
            exit 1
        fi
    fi

    # Verify checksum if available
    verify_checksum "$archive_path" || true
}

verify_checksum() {
    archive_path="$1"

    if ! has sha256sum && ! has shasum; then
        warn "Neither sha256sum nor shasum found — skipping checksum verification."
        return 0
    fi

    info "Verifying checksum..."

    checksum_path="${TMP_DIR}/checksums.txt"
    if ! http_get "$CHECKSUM_URL" > "$checksum_path" 2>/dev/null; then
        if ! http_get "$FALLBACK_CHECKSUM_URL" > "$checksum_path" 2>/dev/null; then
            warn "Could not download checksums — skipping verification."
            return 0
        fi
    fi

    # Compute hash of downloaded file
    if has sha256sum; then
        actual_hash="$(sha256sum "$archive_path" | cut -d' ' -f1)"
    else
        actual_hash="$(shasum -a 256 "$archive_path" | cut -d' ' -f1)"
    fi

    # Look up expected hash
    expected_hash="$(grep "${artifact}" "$checksum_path" | head -1 | cut -d' ' -f1)"

    if [ -z "$expected_hash" ]; then
        warn "Artifact not found in checksums file — skipping verification."
        return 0
    fi

    if [ "$actual_hash" != "$expected_hash" ]; then
        error "Checksum verification FAILED!"
        error "  Expected: ${expected_hash}"
        error "  Actual:   ${actual_hash}"
        error ""
        error "The downloaded file may be corrupted or tampered with."
        error "Please try again or download manually from GitHub."
        rm -f "$archive_path"
        exit 1
    fi

    short_hash="$(printf '%.16s' "$actual_hash")"
    success "Checksum verified ${DIM}(sha256: ${short_hash}...)${RESET}"
}

extract_and_install() {
    info "Installing to ${BOLD}${BIN_DIR}${RESET}..."

    # Create installation directory
    SUDO=""
    if [ ! -d "$BIN_DIR" ]; then
        if ! mkdir -p "$BIN_DIR" 2>/dev/null; then
            if has sudo; then
                SUDO="sudo"
                $SUDO mkdir -p "$BIN_DIR"
            else
                error "Cannot create directory: $BIN_DIR"
                error "Run as root or specify a writable directory:"
                error "  LYNXDB_INSTALL_DIR=~/bin sh -c '\$(curl -fsSL https://lynxdb.org/install.sh)'"
                exit 1
            fi
        fi
    elif [ ! -w "$BIN_DIR" ]; then
        if has sudo; then
            SUDO="sudo"
        else
            error "Cannot write to $BIN_DIR and sudo is not available."
            error "Run as root or specify a writable directory."
            exit 1
        fi
    fi

    # Extract
    extract_dir="${TMP_DIR}/extract"
    mkdir -p "$extract_dir"

    case "$archive_path" in
        *.tar.gz)
            tar -xzf "$archive_path" -C "$extract_dir"
            ;;
        *.zip)
            if has unzip; then
                unzip -qo "$archive_path" -d "$extract_dir"
            elif has 7z; then
                7z x -o"$extract_dir" -y "$archive_path" >/dev/null
            else
                error "Neither 'unzip' nor '7z' found. Cannot extract archive."
                exit 1
            fi
            ;;
    esac

    # Find the binary (might be in a subdirectory)
    binary_path=""
    if [ -f "${extract_dir}/lynxdb" ]; then
        binary_path="${extract_dir}/lynxdb"
    elif [ -f "${extract_dir}/lynxdb.exe" ]; then
        binary_path="${extract_dir}/lynxdb.exe"
    else
        # Search one level deep
        binary_path="$(find "$extract_dir" -name 'lynxdb' -o -name 'lynxdb.exe' | head -1)"
    fi

    if [ -z "$binary_path" ]; then
        error "Could not find lynxdb binary in archive."
        exit 1
    fi

    chmod +x "$binary_path"

    # Move into place
    $SUDO cp -f "$binary_path" "${BIN_DIR}/lynxdb"
    $SUDO chmod 755 "${BIN_DIR}/lynxdb"

    success "Installed ${BOLD}lynxdb${RESET} to ${GREEN}${BIN_DIR}/lynxdb${RESET}"
}

# ─── PATH Configuration ──────────────────────────────────────────────────────

ensure_in_path() {
    if [ -n "${LYNXDB_NO_MODIFY_PATH:-}" ]; then
        return
    fi

    # Check if BIN_DIR is already in PATH
    case ":${PATH}:" in
        *":${BIN_DIR}:"*) return ;; # Already in PATH
    esac

    warn "${BIN_DIR} is not in your \$PATH"
    printf '\n'

    # Detect shell and config file
    shell_name="$(basename "${SHELL:-/bin/sh}")"
    config_file=""
    path_line="export PATH=\"${BIN_DIR}:\$PATH\""

    case "$shell_name" in
        bash)
            if [ -f "$HOME/.bashrc" ]; then
                config_file="$HOME/.bashrc"
            elif [ -f "$HOME/.bash_profile" ]; then
                config_file="$HOME/.bash_profile"
            else
                config_file="$HOME/.bashrc"
            fi
            ;;
        zsh)
            config_file="${ZDOTDIR:-$HOME}/.zshrc"
            ;;
        fish)
            config_file="${XDG_CONFIG_HOME:-$HOME/.config}/fish/config.fish"
            path_line="fish_add_path ${BIN_DIR}"
            ;;
        *)
            config_file="$HOME/.profile"
            ;;
    esac

    # In interactive mode, ask; otherwise just print instructions
    if [ -t 0 ] && [ -t 1 ] && [ -z "${FORCE:-}" ]; then
        printf "  %s Add %s to PATH in %s? %s " \
            "${MAGENTA}?${RESET}" \
            "${BOLD}${BIN_DIR}${RESET}" \
            "${BOLD}${config_file}${RESET}" \
            "${BOLD}[Y/n]${RESET}"

        read -r yn </dev/tty 2>/dev/null || yn="y"
        case "$yn" in
            [nN]*)
                printf '\n'
                info "Skipped. To add manually, run:"
                printf '    %s\n' "$path_line"
                return
                ;;
        esac
    fi

    # Append to config file
    if [ -n "$config_file" ]; then
        # Ensure parent directory exists (for fish)
        config_dir="$(dirname "$config_file")"
        [ -d "$config_dir" ] || mkdir -p "$config_dir"

        # Don't duplicate
        if [ -f "$config_file" ] && grep -qF "$BIN_DIR" "$config_file" 2>/dev/null; then
            debug "PATH entry already exists in $config_file"
            return
        fi

        printf '\n# Added by LynxDB installer\n%s\n' "$path_line" >> "$config_file"
        success "Added ${BIN_DIR} to PATH in ${config_file}"
        info "Restart your shell or run: ${BOLD}source ${config_file}${RESET}"
    fi
}

# ─── Upgrade Detection ────────────────────────────────────────────────────────

check_existing_installation() {
    if has lynxdb; then
        existing_path="$(command -v lynxdb)"
        existing_version="$(lynxdb version --short 2>/dev/null | head -1 || echo 'unknown')"

        if [ "$existing_version" = "$VERSION" ]; then
            success "lynxdb ${VERSION} is already installed at ${existing_path}"
            exit 0
        fi

        info "Upgrading lynxdb: ${DIM}${existing_version}${RESET} → ${GREEN}${VERSION}${RESET}"
        info "  Existing: ${existing_path}"
    fi
}

# ─── Banner & Summary ────────────────────────────────────────────────────────

print_banner() {
    if [ -t 1 ] && [ -z "${QUIET:-}" ]; then
        printf '\n'
        printf '  %s\n' "${BOLD}${CYAN}LynxDB Installer${RESET}"
        printf '  %s\n' "${DIM}The open-source Splunk alternative${RESET}"
        printf '\n'
    fi
}

print_summary() {
    printf '\n'
    printf '  %s\n' "${UNDERLINE}Configuration${RESET}"
    info "${BOLD}Version${RESET}:     ${GREEN}${VERSION}${RESET}"
    info "${BOLD}Platform${RESET}:    ${GREEN}${OS}/${ARCH}${LIBC:+ (${LIBC})}${RESET}"
    info "${BOLD}Install dir${RESET}: ${GREEN}${BIN_DIR}${RESET}"
    printf '\n'
}

print_completion() {
    printf '\n'
    printf '  %s\n' "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    printf '\n'
    success "${BOLD}LynxDB ${VERSION} installed successfully!${RESET}"
    printf '\n'
    printf '  %s\n' "Get started:"
    printf '    %s\n' "${CYAN}lynxdb demo &${RESET}                  ${DIM}# Generate sample data${RESET}"
    printf '    %s\n' "${CYAN}lynxdb query 'level=error'${RESET}     ${DIM}# Run your first query${RESET}"
    printf '    %s\n' "${CYAN}lynxdb --help${RESET}                  ${DIM}# See all commands${RESET}"
    printf '\n'
    if [ -z "${SETUP_SERVER:-}" ]; then
        printf '  %s\n' "Run as a service:"
        printf '    %s\n' "${CYAN}sudo lynxdb install${RESET}            ${DIM}# Set up systemd service + production config${RESET}"
        printf '\n'
    fi
    printf '  %s\n' "${DIM}Docs:    https://lynxdb.org/docs${RESET}"
    printf '  %s\n' "${DIM}Discord: https://discord.gg/lynxdb${RESET}"
    printf '\n'
}

# ─── Argument Parsing ─────────────────────────────────────────────────────────

usage() {
    cat <<EOF
${BOLD}LynxDB Installer${RESET}

${UNDERLINE}Usage${RESET}
  curl -fsSL https://lynxdb.org/install.sh | sh
  curl -fsSL https://lynxdb.org/install.sh | sh -s -- [OPTIONS]

${UNDERLINE}Options${RESET}
  -v, --version VERSION   Install a specific version (e.g. v0.5.0)
  -d, --dir DIRECTORY     Custom installation directory
  -s, --server            Set up as a system service (runs 'lynxdb install')
  -f, --force             Skip confirmation prompts
  -V, --verbose           Enable verbose output
  -q, --quiet             Minimal output
      --no-modify-path    Don't add to PATH
  -h, --help              Show this help message

${UNDERLINE}Environment Variables${RESET}
  LYNXDB_INSTALL_DIR      Installation directory
  LYNXDB_VERSION          Version to install
  LYNXDB_BASE_URL         Override download base URL
  LYNXDB_NO_MODIFY_PATH   Skip PATH modification

${UNDERLINE}Examples${RESET}
  # Install latest
  curl -fsSL https://lynxdb.org/install.sh | sh

  # Install specific version
  curl -fsSL https://lynxdb.org/install.sh | sh -s -- --version v0.5.0

  # Install to custom directory
  curl -fsSL https://lynxdb.org/install.sh | LYNXDB_INSTALL_DIR=~/bin sh

  # Install and set up as a system service
  curl -fsSL https://lynxdb.org/install.sh | sudo sh -s -- --server

EOF
    exit 0
}

parse_args() {
    # Seed from environment variables
    VERSION="${LYNXDB_VERSION:-}"
    INSTALL_DIR="${LYNXDB_INSTALL_DIR:-}"
    BASE_URL="${LYNXDB_BASE_URL:-$DEFAULT_BASE_URL}"
    VERBOSE="${LYNXDB_VERBOSE:-}"
    QUIET="${LYNXDB_QUIET:-}"
    FORCE="${LYNXDB_FORCE:-}"
    SETUP_SERVER=""

    while [ "$#" -gt 0 ]; do
        case "$1" in
            -v|--version)   VERSION="$2"; shift 2 ;;
            -v=*|--version=*) VERSION="${1#*=}"; shift ;;
            -d|--dir)       INSTALL_DIR="$2"; shift 2 ;;
            -d=*|--dir=*)   INSTALL_DIR="${1#*=}"; shift ;;
            -s|--server)    SETUP_SERVER=1; shift ;;
            -f|--force)     FORCE=1; shift ;;
            -V|--verbose)   VERBOSE=1; shift ;;
            -q|--quiet)     QUIET=1; shift ;;
            --no-modify-path) LYNXDB_NO_MODIFY_PATH=1; shift ;;
            -h|--help)      usage ;;
            *)
                error "Unknown option: $1"
                printf '\n' >&2
                usage
                ;;
        esac
    done
}

# ─── Server Setup ─────────────────────────────────────────────────────────────

setup_server_mode() {
    info "Setting up LynxDB as a system service..."
    if [ "$(id -u)" = "0" ]; then
        "${BIN_DIR}/lynxdb" install --yes
    elif has sudo; then
        warn "Root access required for system service setup."
        $SUDO "${BIN_DIR}/lynxdb" install --yes
    else
        warn "Cannot set up system service without root access."
        warn "To set up later: sudo lynxdb install"
    fi
}

# ─── Main ─────────────────────────────────────────────────────────────────────

main() {
    setup_colors
    parse_args "$@"
    print_banner
    detect_http_client
    detect_os
    detect_arch
    detect_libc
    resolve_version
    resolve_install_dir
    check_existing_installation
    print_summary
    build_download_url
    create_tmp_dir
    download_and_verify
    extract_and_install
    ensure_in_path
    if [ -n "${SETUP_SERVER:-}" ]; then
        setup_server_mode
    fi
    print_completion
}

main "$@"
