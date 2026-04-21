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

is_arch_distro() {
  case "$DISTRO_ID" in
    arch|manjaro|endeavouros|garuda|artix|cachyos) return 0 ;;
  esac
  [[ "$DISTRO_LIKE" == *"arch"* ]] && return 0
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

  local arch
  arch="$(dpkg --print-architecture 2>/dev/null || echo amd64)"
  if [ "$arch" != "amd64" ]; then
    info "APT repo only ships amd64 packages; falling back to AppImage on ${arch}"
    install_appimage
    return
  fi

  sudo mkdir -p "$(dirname "$KEYRING")"
  local tmpkey
  tmpkey="$(mktemp)"
  if ! curl -fsSL "$APT_KEY_URL" -o "$tmpkey"; then
    rm -f "$tmpkey"
    err "Failed to download signing key from ${APT_KEY_URL}"
    exit 1
  fi
  sudo gpg --dearmor --yes -o "$KEYRING" < "$tmpkey"
  sudo chmod 0644 "$KEYRING"
  rm -f "$tmpkey"
  ok "Imported signing key"

  echo "deb [arch=${arch} signed-by=${KEYRING}] ${APT_REPO_URL} stable main" \
    | sudo tee "$LIST" >/dev/null
  ok "Added repository"

  info "Updating package index (tablio repo only)..."
  if ! sudo apt-get update \
        -o Dir::Etc::sourcelist="$LIST" \
        -o Dir::Etc::sourceparts="-" \
        -o APT::Get::List-Cleanup="0"; then
    err "apt-get update reported errors/warnings; continuing to install anyway."
    err "If install fails, check ${APT_REPO_URL}/dists/stable/InRelease."
  fi

  info "Installing Tablio..."
  if ! sudo apt-get install -y tablio; then
    err "apt-get install failed. Try: sudo apt-get install tablio   for details."
    exit 1
  fi
  ok "Tablio installed via APT"
}

install_rpm() {
  info "Detected RPM-based distro (${DISTRO_ID})"
  info "Setting up RPM repository..."

  need_cmd curl

  if ! sudo rpm --import "$RPM_KEY_URL"; then
    err "Failed to import signing key from ${RPM_KEY_URL}"
    exit 1
  fi
  ok "Imported signing key"

  if ! sudo curl -fsSL -o /etc/yum.repos.d/tablio.repo "$RPM_REPO_URL"; then
    err "Failed to download repo file from ${RPM_REPO_URL}"
    exit 1
  fi
  ok "Added repository"

  info "Installing Tablio..."
  if command -v dnf &>/dev/null; then
    if ! sudo dnf install -y --repo tablio tablio 2>/dev/null; then
      if ! sudo dnf install -y tablio; then
        err "dnf install failed. Try: sudo dnf install tablio   for details."
        exit 1
      fi
    fi
  elif command -v zypper &>/dev/null; then
    sudo zypper --non-interactive refresh tablio \
      || sudo zypper --non-interactive refresh \
      || err "zypper refresh reported errors/warnings; continuing to install anyway."
    if ! sudo zypper --non-interactive install -y tablio; then
      err "zypper install failed. Try: sudo zypper install tablio   for details."
      exit 1
    fi
  else
    if ! sudo yum install -y tablio; then
      err "yum install failed. Try: sudo yum install tablio   for details."
      exit 1
    fi
  fi

  ok "Tablio installed via RPM"
}

install_aur() {
  info "Detected Arch-based distro (${DISTRO_ID})"

  if command -v yay &>/dev/null; then
    info "Installing from AUR via yay..."
    yay -S --noconfirm tablio-bin
  elif command -v paru &>/dev/null; then
    info "Installing from AUR via paru..."
    paru -S --noconfirm tablio-bin
  else
    info "No AUR helper found, installing with makepkg..."
    need_cmd git
    need_cmd makepkg
    local tmpdir
    tmpdir="$(mktemp -d)"
    git clone https://aur.archlinux.org/tablio-bin.git "$tmpdir/tablio-bin"
    cd "$tmpdir/tablio-bin"
    makepkg -si --noconfirm
    cd -
    rm -rf "$tmpdir"
  fi

  ok "Tablio installed from AUR"
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
  elif is_arch_distro; then
    install_aur
  else
    install_appimage
  fi

  printf "\n${GREEN}${BOLD}  Done!${RESET} Run ${CYAN}tablio${RESET} to get started.\n\n"
}

main
