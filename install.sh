#!/usr/bin/env bash
set -euo pipefail

REPO="dasunNimantha/tablio"
PAGES_URL="https://dasunnimantha.github.io/tablio"
APT_KEY_URL="${PAGES_URL}/apt/key.gpg"
APT_REPO_URL="${PAGES_URL}/apt"
RPM_KEY_URL="${PAGES_URL}/rpm/key.gpg"
RPM_REPO_URL="${PAGES_URL}/rpm/tablio.repo"
KEYRING="/usr/share/keyrings/tablio.gpg"
LIST="/etc/apt/sources.list.d/tablio.list"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

info()  { printf "${CYAN}::${RESET} %s\n" "$*"; }
ok()    { printf "${GREEN}✓${RESET} %s\n" "$*"; }
err()   { printf "${RED}✗${RESET} %s\n" "$*" >&2; }

need_cmd() {
  if ! command -v "$1" &>/dev/null; then
    err "Required command not found: $1"
    exit 1
  fi
}

detect_distro() {
  if [ -f /etc/os-release ]; then
    . /etc/os-release
    DISTRO_ID="${ID:-unknown}"
    DISTRO_LIKE="${ID_LIKE:-}"
  elif command -v lsb_release &>/dev/null; then
    DISTRO_ID="$(lsb_release -si | tr '[:upper:]' '[:lower:]')"
    DISTRO_LIKE=""
  else
    DISTRO_ID="unknown"
    DISTRO_LIKE=""
  fi
}

is_apt_distro() {
  case "$DISTRO_ID" in
    ubuntu|debian|linuxmint|pop|elementary|zorin|neon|kali) return 0 ;;
  esac
  [[ "$DISTRO_LIKE" == *"debian"* || "$DISTRO_LIKE" == *"ubuntu"* ]] && return 0
  return 1
}

is_rpm_distro() {
  case "$DISTRO_ID" in
    fedora|rhel|centos|rocky|alma|opensuse*|sles) return 0 ;;
  esac
  [[ "$DISTRO_LIKE" == *"fedora"* || "$DISTRO_LIKE" == *"rhel"* || "$DISTRO_LIKE" == *"suse"* ]] && return 0
  return 1
}

latest_asset_url() {
  local pattern="$1"
  curl -fsSL "https://api.github.com/repos/${REPO}/releases" \
    | grep -oP "\"browser_download_url\":\s*\"[^\"]*${pattern}[^\"]*\"" \
    | head -1 \
    | cut -d'"' -f4
}

install_apt() {
  info "Detected Debian/Ubuntu-based distro (${DISTRO_ID})"
  info "Setting up APT repository..."

  need_cmd curl
  need_cmd gpg

  sudo mkdir -p "$(dirname "$KEYRING")"
  curl -fsSL "$APT_KEY_URL" | sudo gpg --dearmor --yes -o "$KEYRING"
  ok "Imported signing key"

  echo "deb [signed-by=${KEYRING}] ${APT_REPO_URL} stable main" \
    | sudo tee "$LIST" >/dev/null
  ok "Added repository"

  sudo apt-get update -qq
  sudo apt-get install -y tablio
  ok "Tablio installed via APT"
}

install_rpm() {
  info "Detected RPM-based distro (${DISTRO_ID})"
  info "Setting up RPM repository..."

  need_cmd curl

  sudo rpm --import "$RPM_KEY_URL"
  ok "Imported signing key"

  sudo curl -fsSL -o /etc/yum.repos.d/tablio.repo "$RPM_REPO_URL"
  ok "Added repository"

  if command -v dnf &>/dev/null; then
    sudo dnf install -y tablio
  elif command -v zypper &>/dev/null; then
    sudo zypper refresh
    sudo zypper install -y tablio
  else
    sudo yum install -y tablio
  fi

  ok "Tablio installed via RPM"
}

install_appimage() {
  info "Distro not directly supported for package install (${DISTRO_ID})"
  info "Downloading latest AppImage..."

  need_cmd curl

  local url
  url="$(latest_asset_url '\.AppImage')"
  if [ -z "$url" ]; then
    err "No AppImage asset found in the latest release"
    exit 1
  fi

  local dest="${HOME}/.local/bin"
  mkdir -p "$dest"

  curl -fSL --progress-bar -o "${dest}/Tablio.AppImage" "$url"
  chmod +x "${dest}/Tablio.AppImage"
  ok "Tablio installed to ${dest}/Tablio.AppImage"

  if [[ ":$PATH:" != *":${dest}:"* ]]; then
    info "Add ${dest} to your PATH if it's not already there"
  fi
}

main() {
  printf "\n${BOLD}  Tablio Installer${RESET}\n\n"

  detect_distro

  if is_apt_distro; then
    install_apt
  elif is_rpm_distro; then
    install_rpm
  else
    install_appimage
  fi

  printf "\n${GREEN}${BOLD}  Done!${RESET} Run ${CYAN}tablio${RESET} to get started.\n\n"
}

main
