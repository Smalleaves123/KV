#!/usr/bin/env bash
set -euo pipefail

CLANG_FORMAT="${CLANG_FORMAT:-}"
if [[ -z "${CLANG_FORMAT}" ]] && command -v clang-format >/dev/null 2>&1; then
  CLANG_FORMAT="$(command -v clang-format)"
fi
if [[ -z "${CLANG_FORMAT}" && -x "/opt/homebrew/opt/llvm/bin/clang-format" ]]; then
  CLANG_FORMAT="/opt/homebrew/opt/llvm/bin/clang-format"
fi
if [[ -z "${CLANG_FORMAT}" ]]; then
  echo "clang-format is required; install it or run the build without formatting" >&2
  exit 1
fi

files=()
while IFS= read -r file; do
  files+=("${file}")
done < <(if [[ "${FORMAT_SCOPE:-all}" == "added" ]]; then
  git ls-files --others --exclude-standard |
    rg '^(include|src|apps|tests|examples|bench|tools)/.*\.(cpp|h|tpp)$' || true
elif [[ "${FORMAT_SCOPE:-all}" == "changed" ]]; then
  {
    git diff --name-only --diff-filter=ACMRTUXB
    git ls-files --others --exclude-standard
  } | rg '^(include|src|apps|tests|examples|bench|tools)/.*\.(cpp|h|tpp)$' || true
else
  rg --files include src apps tests examples bench tools |
    rg '\.(cpp|h|tpp)$'
fi)

if ((${#files[@]} == 0)); then
  echo "No C++ files selected for formatting"
  exit 0
fi

if [[ "${CHECK:-0}" == "1" ]]; then
  "${CLANG_FORMAT}" --dry-run --Werror "${files[@]}"
else
  "${CLANG_FORMAT}" -i "${files[@]}"
fi
