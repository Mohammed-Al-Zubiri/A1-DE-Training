#!/usr/bin/env bash
set -euo pipefail

PKG_NAME="scraping-service"
VERSION="${1:-1.0.0-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_ROOT="${SCRIPT_DIR}/build"
PKG_ROOT="${BUILD_ROOT}/${PKG_NAME}_${VERSION}"
DEB_OUT="${BUILD_ROOT}/${PKG_NAME}_${VERSION}.deb"

rm -rf "${PKG_ROOT}" "${DEB_OUT}"
mkdir -p "${PKG_ROOT}/DEBIAN"
mkdir -p "${PKG_ROOT}/opt/scraping-service"
mkdir -p "${PKG_ROOT}/usr/bin"
mkdir -p "${PKG_ROOT}/lib/systemd/system"
mkdir -p "${PKG_ROOT}/etc/scraping-service"
mkdir -p "${BUILD_ROOT}"

cp "${SCRIPT_DIR}/debian/DEBIAN/control" "${PKG_ROOT}/DEBIAN/control"
sed -i "s/^Version: .*/Version: ${VERSION}/" "${PKG_ROOT}/DEBIAN/control"

for maint_script in postinst prerm postrm; do
    cp "${SCRIPT_DIR}/debian/DEBIAN/${maint_script}" "${PKG_ROOT}/DEBIAN/${maint_script}"
    chmod 755 "${PKG_ROOT}/DEBIAN/${maint_script}"
done

cp -r "${PROJECT_ROOT}/src" "${PKG_ROOT}/opt/scraping-service/"
cp "${PROJECT_ROOT}/requirements.txt" "${PKG_ROOT}/opt/scraping-service/"

install -m 755 "${PROJECT_ROOT}/deploy/bin/scraping-service-api" "${PKG_ROOT}/usr/bin/scraping-service-api"
install -m 755 "${PROJECT_ROOT}/deploy/bin/scraping-service-scrape" "${PKG_ROOT}/usr/bin/scraping-service-scrape"
install -m 644 "${PROJECT_ROOT}/deploy/systemd/scraping-service.service" "${PKG_ROOT}/lib/systemd/system/scraping-service.service"
install -m 644 "${PROJECT_ROOT}/deploy/config/scraping-service.env" "${PKG_ROOT}/etc/scraping-service/scraping-service.env"

dpkg-deb --build --root-owner-group "${PKG_ROOT}" "${DEB_OUT}"
echo "Built package: ${DEB_OUT}"
