#!/bin/sh
# LynxDB Installer
# https://lynxdb.org/install.sh
#
# Usage:
#   curl -fsSL https://lynxdb.org/install.sh | sh
#   curl -fsSL https://lynxdb.org/install.sh | sh -s -- --version 26.11.1.80
#   curl -fsSL https://lynxdb.org/install.sh | LYNXDB_INSTALL_DIR=/opt/lynxdb sh
#
# Environment variables:
#   LYNXDB_INSTALL_DIR  - Installation directory (default: /usr/local/bin or ~/.local/bin)
#   LYNXDB_VERSION      - Specific version to install (default: the newest of the channel)
#   LYNXDB_CHANNEL      - Release channel: stable or lts (default: stable)
#   LYNXDB_BASE_URL     - Override download base URL
#   LYNXDB_NO_MODIFY_PATH - If set, skip PATH modification
#
# This script is designed to be:
#   - POSIX sh compatible (no bash-isms)
#   - Simple and auditable
#   - Safe to pipe from curl

set -eu


DEFAULT_BASE_URL="https://dl.lynxdb.org"
RELEASES_URL="https://github.com/lynxbase/lynxdb/releases"


setup_colors() {
    if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
        BOLD="$(tput bold 2>/dev/null || printf '')"
        DIM="$(tput dim 2>/dev/null || printf '')"
        UNDERLINE="$(tput smul 2>/dev/null || printf '')"
        RED="$(tput setaf 1 2>/dev/null || printf '')"
        GREEN="$(tput setaf 2 2>/dev/null || printf '')"
        YELLOW="$(tput setaf 3 2>/dev/null || printf '')"
        MAGENTA="$(tput setaf 5 2>/dev/null || printf '')"
        CYAN="$(tput setaf 6 2>/dev/null || printf '')"
        RESET="$(tput sgr0 2>/dev/null || printf '')"
    else
        BOLD="" DIM="" UNDERLINE=""
        RED="" GREEN="" YELLOW="" MAGENTA="" CYAN="" RESET=""
    fi
}


info()      { printf '%s\n' "${BOLD}${CYAN}▸${RESET} $*"; }
success()   { printf '%s\n' "${GREEN}✔${RESET} $*"; }
warn()      { printf '%s\n' "${YELLOW}!${RESET} $*" >&2; }
error()     { printf '%s\n' "${RED}✖${RESET} $*" >&2; }
debug()     { [ -z "${VERBOSE:-}" ] || printf '%s\n' "${DIM}  $*${RESET}"; }


has() { command -v "$1" >/dev/null 2>&1; }

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


detect_os() {
    os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    case "$os" in
        linux*)   OS="linux" ;;
        darwin*)  OS="darwin" ;;
        msys*|mingw*|cygwin*) OS="windows" ;;
        *)
            error "Unsupported operating system: $os"
            error "LynxDB supports Linux, macOS, and Windows (via WSL/Git Bash)."
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

validate_channel_request() {
    case "$CHANNEL" in
        stable|lts) ;;
        *)
            error "Invalid channel: $CHANNEL"
            error "Use --channel stable or --channel lts."
            exit 1
            ;;
    esac
}

# A version reads year.month.patch.build. A release tag, v26.11.1.80-stable,
# is accepted in its place.
normalize_version() {
    VERSION="${VERSION#v}"
    case "$VERSION" in
        *-stable|*-lts|*-prestable) VERSION="${VERSION%-*}" ;;
    esac
}

# The newest version of the channel, unless one was asked for, and the release
# tag that holds its files.
resolve_version() {
    if [ -n "${VERSION:-}" ]; then
        normalize_version
        debug "Using specified version: $VERSION"
    else
        info "Resolving latest ${CHANNEL} version..."
        if ! VERSION="$(http_get "${BASE_URL}/channels/${CHANNEL}" 2>/dev/null)"; then
            error "Could not read ${BASE_URL}/channels/${CHANNEL}."
            error "The ${CHANNEL} channel has no release yet, or the download site cannot be reached."
            error "Please specify a version manually: --version 26.11.1.80"
            exit 1
        fi
        debug "Latest version of the channel: $VERSION"
    fi

    # A build between releases adds its distance and commit: 26.11.1.80+5.gabc1234.
    case "$VERSION" in
        ''|*[!0-9a-z.+]*)
            error "Not a LynxDB version: ${VERSION}"
            error "A version reads year.month.patch.build, for example 26.11.1.80."
            exit 1
            ;;
    esac

    if ! TAG="$(http_get "${BASE_URL}/versions/${VERSION}" 2>/dev/null)"; then
        error "Version ${VERSION} is not published."
        error "Releases: ${UNDERLINE}${RELEASES_URL}${RESET}"
        exit 1
    fi

    case "$TAG" in
        "v${VERSION}-stable"|"v${VERSION}-lts"|"v${VERSION}-prestable") ;;
        *)
            error "${BASE_URL}/versions/${VERSION} does not name a release of ${VERSION}."
            exit 1
            ;;
    esac
    RESOLVED_CHANNEL="${TAG##*-}"
    debug "Release tag: $TAG"
}


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


build_download_url() {
    # Archive name: lynxdb-{version}-{os}-{arch}.tar.gz, .zip on Windows
    name="lynxdb-${VERSION}-${OS}-${ARCH}"
    if [ "$OS" = "windows" ]; then
        artifact="${name}.zip"
        binary="lynxdb.exe"
    else
        artifact="${name}.tar.gz"
        binary="lynxdb"
    fi

    DOWNLOAD_URL="${BASE_URL}/dist/${TAG}/${artifact}"
    CHECKSUM_URL="${BASE_URL}/dist/${TAG}/SHA256SUMS"

    debug "Download URL: $DOWNLOAD_URL"
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

    if ! http_download "$DOWNLOAD_URL" "$archive_path"; then
        error "Download failed."
        printf '\n' >&2
        error "This could mean:"
        error "  • No build of ${VERSION} for ${OS}/${ARCH}"
        error "  • Network connectivity issues"
        printf '\n' >&2
        error "Available builds: ${UNDERLINE}${RELEASES_URL}/tag/${TAG}${RESET}"
        exit 1
    fi

    verify_checksum "$archive_path"
}

verify_checksum() {
    archive_path="$1"

    if ! has sha256sum && ! has shasum; then
        error "Neither 'sha256sum' nor 'shasum' found, so the download cannot be verified."
        error "Please install one of them and try again."
        exit 1
    fi

    info "Verifying checksum..."

    checksum_path="${TMP_DIR}/SHA256SUMS"
    if ! http_get "$CHECKSUM_URL" > "$checksum_path" 2>/dev/null; then
        error "Could not download ${CHECKSUM_URL}."
        exit 1
    fi

    # Compute hash of downloaded file
    if has sha256sum; then
        actual_hash="$(sha256sum "$archive_path" | cut -d' ' -f1)"
    else
        actual_hash="$(shasum -a 256 "$archive_path" | cut -d' ' -f1)"
    fi

    # Look up expected hash
    expected_hash="$(awk -v name="$artifact" '$2 == name { print $1 }' "$checksum_path")"

    if [ -z "$expected_hash" ]; then
        error "SHA256SUMS of ${TAG} does not list ${artifact}."
        exit 1
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

    # The archive holds one directory, named after itself.
    binary_path="${extract_dir}/${name}/${binary}"
    if [ ! -f "$binary_path" ]; then
        error "Could not find ${binary} in the archive."
        exit 1
    fi

    # Move into place
    $SUDO cp -f "$binary_path" "${BIN_DIR}/${binary}"
    $SUDO chmod 755 "${BIN_DIR}/${binary}"

    success "Installed ${BOLD}lynxdb${RESET} to ${GREEN}${BIN_DIR}/${binary}${RESET}"
}


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


print_banner() {
    if [ -t 1 ] && [ -z "${QUIET:-}" ]; then
        printf '\n'
        printf '  %s\n' "${BOLD}${CYAN}LynxDB Installer${RESET}"
        printf '  %s\n' "${DIM}A log analytics database in one binary${RESET}"
        printf '\n'
    fi
}

print_summary() {
    printf '\n'
    printf '  %s\n' "${UNDERLINE}Configuration${RESET}"
    info "${BOLD}Version${RESET}:     ${GREEN}${VERSION}${RESET}"
    info "${BOLD}Channel${RESET}:     ${GREEN}${RESOLVED_CHANNEL}${RESET}"
    info "${BOLD}Platform${RESET}:    ${GREEN}${OS}/${ARCH}${RESET}"
    info "${BOLD}Install dir${RESET}: ${GREEN}${BIN_DIR}${RESET}"
    if [ "$RESOLVED_CHANNEL" = "prestable" ]; then
        warn "You are installing a release candidate. It may contain regressions."
    fi
    printf '\n'
}

print_completion() {
    printf '\n'
    printf '  %s\n' "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    printf '\n'
    success "${BOLD}LynxDB ${VERSION} installed successfully!${RESET}"
    printf '\n'
    printf '  %s\n' "Get started:"
    printf '    %s\n' "${CYAN}lynxdb query --file app.log 'source=main | stats count() by level'${RESET}"
    printf '    %s\n' "${CYAN}lynxdb server${RESET}                  ${DIM}# Start the server${RESET}"
    printf '    %s\n' "${CYAN}lynxdb --help${RESET}                  ${DIM}# See all commands${RESET}"
    printf '\n'
    printf '  %s\n' "${DIM}Docs:    https://docs.lynxdb.org/${RESET}"
    printf '  %s\n' "${DIM}Discord: https://discord.gg/RgggCFdgWK${RESET}"
    printf '\n'
}


usage() {
    cat <<EOF
${BOLD}LynxDB Installer${RESET}

${UNDERLINE}Usage${RESET}
  curl -fsSL https://lynxdb.org/install.sh | sh
  curl -fsSL https://lynxdb.org/install.sh | sh -s -- [OPTIONS]

${UNDERLINE}Options${RESET}
  -v, --version VERSION   Install a specific version (e.g. 26.11.1.80)
      --channel CHANNEL   Release channel: stable or lts
  -d, --dir DIRECTORY     Custom installation directory
  -f, --force             Skip confirmation prompts
  -V, --verbose           Enable verbose output
  -q, --quiet             Minimal output
      --no-modify-path    Don't add to PATH
  -h, --help              Show this help message

${UNDERLINE}Environment Variables${RESET}
  LYNXDB_INSTALL_DIR      Installation directory
  LYNXDB_VERSION          Version to install
  LYNXDB_CHANNEL          Release channel: stable or lts
  LYNXDB_BASE_URL         Override download base URL
  LYNXDB_NO_MODIFY_PATH   Skip PATH modification

${UNDERLINE}Examples${RESET}
  # Install latest
  curl -fsSL https://lynxdb.org/install.sh | sh

  # Install specific version
  curl -fsSL https://lynxdb.org/install.sh | sh -s -- --version 26.11.1.80

  # Install the newest release of the lts channel
  curl -fsSL https://lynxdb.org/install.sh | sh -s -- --channel lts

  # Install to custom directory
  curl -fsSL https://lynxdb.org/install.sh | LYNXDB_INSTALL_DIR=~/bin sh

EOF
}

parse_args() {
    # Seed from environment variables
    VERSION="${LYNXDB_VERSION:-}"
    INSTALL_DIR="${LYNXDB_INSTALL_DIR:-}"
    BASE_URL="${LYNXDB_BASE_URL:-$DEFAULT_BASE_URL}"
    BASE_URL="${BASE_URL%/}"
    CHANNEL="${LYNXDB_CHANNEL:-stable}"
    VERBOSE="${LYNXDB_VERBOSE:-}"
    QUIET="${LYNXDB_QUIET:-}"
    FORCE="${LYNXDB_FORCE:-}"

    while [ "$#" -gt 0 ]; do
        case "$1" in
            -v|--version)   VERSION="$2"; shift 2 ;;
            -v=*|--version=*) VERSION="${1#*=}"; shift ;;
            --channel)      CHANNEL="$2"; shift 2 ;;
            --channel=*)    CHANNEL="${1#*=}"; shift ;;
            -d|--dir)       INSTALL_DIR="$2"; shift 2 ;;
            -d=*|--dir=*)   INSTALL_DIR="${1#*=}"; shift ;;
            -f|--force)     FORCE=1; shift ;;
            -V|--verbose)   VERBOSE=1; shift ;;
            -q|--quiet)     QUIET=1; shift ;;
            --no-modify-path) LYNXDB_NO_MODIFY_PATH=1; shift ;;
            -h|--help)      usage; exit 0 ;;
            *)
                error "Unknown option: $1"
                printf '\n' >&2
                usage >&2
                exit 2
                ;;
        esac
    done
}


main() {
    setup_colors
    parse_args "$@"
    validate_channel_request
    print_banner
    detect_http_client
    detect_os
    detect_arch
    resolve_version
    resolve_install_dir
    check_existing_installation
    print_summary
    build_download_url
    create_tmp_dir
    download_and_verify
    extract_and_install
    ensure_in_path
    print_completion
}

main "$@"
