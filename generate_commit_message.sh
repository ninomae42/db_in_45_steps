#!/usr/bin/env bash
#
# generate-commit-msg.sh
# git diff の内容をもとに Claude Code でセマンティックコミットメッセージを生成するスクリプト
#
# 使い方:
#   ./generate-commit-msg.sh            # ステージング済みの差分からメッセージ生成
#   ./generate-commit-msg.sh --all      # 全差分（未ステージング含む）からメッセージ生成
#   ./generate-commit-msg.sh --commit   # メッセージ生成後、そのままコミット実行
#
# 前提条件:
#   - claude (Claude Code CLI) がインストール済みであること
#   - git リポジトリ内で実行すること
#

set -euo pipefail

# ─────────────────────────────────────
# 定数・デフォルト設定
# ─────────────────────────────────────
LANG_PROMPT="日本語"  # コミットメッセージの言語（"English" に変更可）
AUTO_COMMIT=true
DIFF_MODE="staged"

# ─────────────────────────────────────
# 引数パース
# ─────────────────────────────────────
for arg in "$@"; do
  case "$arg" in
    --all)     DIFF_MODE="all" ;;
    --commit)  AUTO_COMMIT=true ;;
    --en)      LANG_PROMPT="English" ;;
    --help|-h)
      echo "Usage: $(basename "$0") [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --all      ステージング済み + 未ステージングの全差分を対象にする"
      echo "  --commit   生成したメッセージでそのままコミットする"
      echo "  --en       コミットメッセージを英語で生成する"
      echo "  -h,--help  このヘルプを表示"
      exit 0
      ;;
    *)
      echo "Error: 不明なオプション '$arg'" >&2
      echo "  $(basename "$0") --help でヘルプを表示" >&2
      exit 1
      ;;
  esac
done

# ─────────────────────────────────────
# 前提チェック
# ─────────────────────────────────────

# git リポジトリ内か確認
if ! git rev-parse --is-inside-work-tree &>/dev/null; then
  echo "Error: git リポジトリ内で実行してください。" >&2
  exit 1
fi

# Claude Code CLI の存在確認
if ! command -v claude &>/dev/null; then
  echo "Error: claude (Claude Code CLI) が見つかりません。" >&2
  echo "  npm install -g @anthropic-ai/claude-code でインストールしてください。" >&2
  exit 1
fi

# ─────────────────────────────────────
# diff 取得
# ─────────────────────────────────────
if [[ "$DIFF_MODE" == "all" ]]; then
  DIFF_OUTPUT=$(git diff HEAD 2>/dev/null || git diff)
else
  DIFF_OUTPUT=$(git diff --cached)
fi

if [[ -z "$DIFF_OUTPUT" ]]; then
  if [[ "$DIFF_MODE" == "staged" ]]; then
    echo "Error: ステージングされた変更がありません。" >&2
    echo "  git add で変更をステージングするか、--all オプションを使用してください。" >&2
  else
    echo "Error: 差分が見つかりません。" >&2
  fi
  exit 1
fi

# diff が大きすぎる場合は stat のみにフォールバック
DIFF_LINES=$(echo "$DIFF_OUTPUT" | wc -l)
if [[ "$DIFF_LINES" -gt 2000 ]]; then
  echo "Warning: 差分が大きいため (${DIFF_LINES} 行)、要約モードで実行します。" >&2
  DIFF_STAT=$(git diff --cached --stat 2>/dev/null || git diff --stat)
  DIFF_OUTPUT="[差分が大きいため stat のみ表示]
${DIFF_STAT}

[先頭500行の diff]
$(echo "$DIFF_OUTPUT" | head -500)"
fi

# ─────────────────────────────────────
# プロンプト構築
# ─────────────────────────────────────
PROMPT="あなたは熟練のソフトウェアエンジニアです。
以下の git diff を分析し、Semantic Commit Messages の形式でコミットメッセージを1つ生成してください。

## Semantic Commit Messages のフォーマット

\`\`\`
<type>(<scope>): <subject>
\`\`\`

### type（必須）
- feat:     新機能
- fix:      バグ修正
- docs:     ドキュメントのみの変更
- style:    コードの意味に影響しない変更（空白、フォーマット、セミコロンなど）
- refactor: バグ修正でも機能追加でもないコード変更
- perf:     パフォーマンス改善
- test:     テストの追加・修正
- chore:    ビルドプロセスや補助ツール、ライブラリの変更
- ci:       CI関連の変更
- build:    ビルドシステムや外部依存関係の変更
- revert:   以前のコミットの取り消し

### scope（任意）
変更の影響範囲を括弧内に記載（例: auth, api, ui）

### subject（必須）
- 命令形で記述
- 先頭を大文字にしない
- 末尾にピリオドをつけない
- ${LANG_PROMPT}で記述

## 出力フォーマット

以下の形式で出力すること。各セクションの区切りには \`---\` を使用する。
装飾（バッククォート、コードブロック、マークダウン見出し等）は一切つけないこと。

\`\`\`
<type>(<scope>): <subject>

目的: この変更の目的を1〜2文で簡潔に説明する。
影響: 既存コードへの影響（破壊的変更の有無、副作用など）を記載する。
---
変更点をファイル単位・関数単位で箇条書きにする。
各項目は具体的な変更内容を記述する（「〜を変更」「〜を追加」「〜を削除」など）。
---
要約: 変更全体を1文で要約する。
\`\`\`

## ルール
- 上記フォーマットを厳密に守ること
- 1行目のコミットメッセージは72文字以内
- 複数の変更がある場合、最も重要な変更を1行目にまとめる
- ${LANG_PROMPT}で記述する
- バッククォートやマークダウン記法で囲まないこと

## git diff:

\`\`\`diff
${DIFF_OUTPUT}
\`\`\`"

# ─────────────────────────────────────
# Claude Code でメッセージ生成
# ─────────────────────────────────────
echo "🤖 コミットメッセージを生成中..." >&2

COMMIT_MSG=$(echo "$PROMPT" | claude --print 2>/dev/null)

if [[ -z "$COMMIT_MSG" ]]; then
  echo "Error: コミットメッセージの生成に失敗しました。" >&2
  exit 1
fi

# バッククォートやコードブロックの装飾を除去
COMMIT_MSG=$(echo "$COMMIT_MSG" | sed '/^```/d' | sed 's/^[[:space:]]*//')

# ─────────────────────────────────────
# 結果出力・コミット
# ─────────────────────────────────────

# 1行目（コミットメッセージ本体）を抽出
COMMIT_SUBJECT=$(echo "$COMMIT_MSG" | head -1)

echo "" >&2
echo "━━━ コミットメッセージ ━━━" >&2
echo "$COMMIT_SUBJECT" >&2
echo "" >&2
echo "━━━ 変更内容 ━━━" >&2
echo "$COMMIT_MSG" | tail -n +2 | sed '/./,$!d' >&2
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" >&2

if [[ "$AUTO_COMMIT" == true ]]; then
  # --all の場合は全変更をステージング
  if [[ "$DIFF_MODE" == "all" ]]; then
    git add -A
  fi

  echo "" >&2
  echo "📝 コミットを実行しますか？ [Y/n] " >&2
  read -r CONFIRM
  if [[ "$CONFIRM" =~ ^[Nn] ]]; then
    echo "コミットをキャンセルしました。" >&2
    echo "以下のコマンドで手動コミットできます:" >&2
    echo "  git commit -m \"${COMMIT_SUBJECT}\"" >&2
    exit 0
  fi

  git commit -m "$COMMIT_MSG"
  echo "✅ コミットしました！" >&2
else
  echo "" >&2
  echo "💡 以下のコマンドでコミットできます:" >&2
  echo "  git commit -m \"${COMMIT_SUBJECT}\"" >&2
fi
