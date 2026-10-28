# hermes-traq

Hermes Gateway から traQ のチャンネルと DM でテキスト会話するプラットフォームプラグインです。traQ WebSocket Bot を使います。

## セットアップ

1. Hermes Agent の環境で `python -c 'import aiohttp; print(aiohttp.__version__)'` を実行し、未インストールの場合は Hermes の Python 環境に `pip install aiohttp` を実行します。公式 Docker イメージではインストール済みです。対応 API は Hermes Agent のコミット `e5ea5fd9ccdf2913c0b21b24eb3d22b321d2e2a6` を基準にしています。
2. traQ で WebSocket モードの Bot を作り、`MESSAGE_CREATED` と `DIRECT_MESSAGE_CREATED` を購読します。チャンネルで使う場合は Bot をそのチャンネルに参加させます。
3. このリポジトリの `traq` ディレクトリを `~/.hermes/plugins/traq` に配置し、`hermes plugins enable traq-platform` を実行します。
4. `~/.hermes/.env` に次を設定し、`hermes gateway` を起動します。

```dotenv
TRAQ_BOT_TOKEN=Botのアクセストークン
# 省略すると https://q.trap.jp
TRAQ_ORIGIN=https://q.trap.jp
# traQのユーザーUUIDをカンマ区切りで指定。空の場合はすべてのユーザーを許可
TRAQ_ALLOWED_USERS=
# false の場合、参加中のすべてのチャンネルでメンション不要
TRAQ_REQUIRE_MENTION=true
# ここに指定したチャンネルUUIDではメンション不要
TRAQ_FREE_RESPONSE_CHANNELS=
```

DM は常にメンション不要です。許可リストはチャンネルと DM の両方に適用されます。空の許可リストは誰でも利用できるため、Bot にツール実行を許可している場合は用途に応じて設定してください。ほかの Bot の発言は処理しません。

処理開始時に `eyes`、正常終了時に `white_check_mark`、失敗時に `x` スタンプを押します。スタンプがない場合はログに記録して会話を続行します。ツール進捗は Hermes の表示設定に従い、進捗メッセージの編集に対応します。たとえば `config.yaml` で `display.tool_progress: all` と `display.tool_progress_grouping: accumulate` を指定できます。セッション、コマンド、処理中の追加メッセージは Hermes Gateway の標準動作に従います。

チャンネルと DM で、画像・音声・動画・文書などのファイルを受信し、Hermes が生成したファイルを添付して送信できます。受信ファイルは 1 件 32 MiB 以下で、テキストファイルは 100 KiB 以下の場合に内容も会話に渡します。大きすぎるファイルや取得に失敗したファイルはその旨を会話に渡します。音声は通常のファイル添付として送信されます。

## テスト

Python 3.12 以降と uv を用意し、`uv sync` の後に `uv run pytest -q` で通信と振り分けをテストできます。

`docker/docker-run.sh` は `docker run` で隔離された MariaDB と traQ、Hermes の公式イメージを起動します。実際の Bot イベントによるチャンネルと DM の送受信、添付、編集、スタンプを検証した後、テスト用モデル API を使って Hermes Gateway のツール実行と進捗編集まで確認します。終了時にテスト用コンテナとネットワークを削除します。

Docker Compose を使う場合は `docker compose -f docker/compose.yaml up --abort-on-container-exit --exit-code-from smoke` で同じテストを実行できます。終了後は `docker compose -f docker/compose.yaml down -v` でテストデータを削除します。
