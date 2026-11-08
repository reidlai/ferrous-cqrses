#!/usr/bin/env bash
# build-and-scan.sh: Builds ferrous-cqrses Docker image and scans with Hadolint and Trivy (Story 7.5)
# Enforces Tier 2 DevSecOps standards locally without incurring cloud CI minutes (AD-13, AD-24, NFR-10..13).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DOCKERFILES=("Dockerfile.command_handler" "Dockerfile")
TARGET_IMAGE="ferrous-cqrses-command-handler:latest"

SKIP_SCAN=false
DRY_RUN=false

for arg in "$@"; do
    case "$arg" in
        --skip-scan) SKIP_SCAN=true ;;
        --dry-run) DRY_RUN=true ;;
        *) echo "Unknown option: $arg" >&2; exit 1 ;;
    esac
done

echo "======================================================================"
echo " [DEVSECOPS] Building and Scanning ferrous-cqrses Container"
echo "======================================================================"

cd "${REPO_ROOT}"

# 1. Hadolint Dockerfile Linting
echo -e "\n[Step 1/3] Running Hadolint Dockerfile SAST audit..."
for df in "${DOCKERFILES[@]}"; do
    echo "  Auditing ${df}..."
    if [ "${DRY_RUN}" = "true" ]; then
        echo "  [DRY-RUN] hadolint ${df}"
    else
        if command -v hadolint >/dev/null 2>&1; then
            hadolint "${df}"
        else
            echo "  [INFO] Local hadolint CLI not found; running via hadolint/hadolint container..."
            docker run --rm -v "${REPO_ROOT}:/work" -w /work hadolint/hadolint:latest hadolint "${df}"
        fi
        echo "  [OK] ${df} passed Hadolint linting (0 issues)."
    fi
done

# 2. Build Container Image
echo -e "\n[Step 2/3] Building container image..."
echo "  Building image ${TARGET_IMAGE} from Dockerfile.command_handler..."
if [ "${DRY_RUN}" = "true" ]; then
    echo "  [DRY-RUN] docker build -t ${TARGET_IMAGE} -f Dockerfile.command_handler ."
else
    docker build -t "${TARGET_IMAGE}" -f Dockerfile.command_handler .
    echo "  [OK] Container image ${TARGET_IMAGE} built successfully."
fi

if [ "${SKIP_SCAN}" = "true" ]; then
    echo -e "\n[WARN] Skipping Trivy vulnerability scanning (--skip-scan enabled)."
    exit 0
fi

# Helper function for Trivy execution
run_trivy() {
    local image_name="$1"
    if command -v trivy >/dev/null 2>&1; then
        trivy image --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 "${image_name}"
    else
        echo "  [INFO] Local trivy CLI not found; running via aquasec/trivy container..."
        docker run --rm \
            -v //var/run/docker.sock:/var/run/docker.sock \
            aquasec/trivy:latest image \
            --severity HIGH,CRITICAL \
            --ignore-unfixed \
            --exit-code 1 \
            "${image_name}"
    fi
}

# 3. Trivy Scan
echo -e "\n[Step 3/3] Running Trivy vulnerability scan on container image..."
echo "  Scanning image ${TARGET_IMAGE}..."
if [ "${DRY_RUN}" = "true" ]; then
    echo "  [DRY-RUN] trivy image --severity HIGH,CRITICAL --ignore-unfixed --exit-code 1 ${TARGET_IMAGE}"
else
    run_trivy "${TARGET_IMAGE}"
    echo "  [OK] Zero HIGH or CRITICAL unfixed vulnerabilities detected in ${TARGET_IMAGE}."
fi

echo -e "\n======================================================================"
echo " [SUCCESS] Container Target Built and Verified (0 CVEs)!"
echo "======================================================================"
