# The Versity S3 Gateway:<br/>A High-Performance S3 Translation Service

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://github.com/versity/versitygw/blob/assets/assets/logo-white.svg">
  <source media="(prefers-color-scheme: light)" srcset="https://github.com/versity/versitygw/blob/assets/assets/logo.svg">
  <a href="https://www.versity.com"><img alt="Versity Software logo image." src="https://github.com/versity/versitygw/blob/assets/assets/logo.svg"></a>
</picture>

 [![Apache V2 License](https://img.shields.io/badge/license-Apache%20V2-blue.svg)](https://github.com/versity/versitygw/blob/main/LICENSE) [![Go Reference](https://pkg.go.dev/badge/github.com/versity/versitygw.svg)](https://pkg.go.dev/github.com/versity/versitygw)

### Binary release builds
Download [latest release](https://github.com/versity/versitygw/releases)
 | Linux amd64/arm64 | MacOS amd64/arm64 | BSD amd64/arm64 | Windows amd64/arm64 |
 |:-----------:|:-----------:|:-----------:|:-----------:|
 |    ✔️    |  ✔️  |   ✔️   |  ✔️   |
### Use Cases
* Turn your local filesystem into an S3 server with a single command!
* Proxy S3 requests to S3 storage
* Simple to deploy S3 server with a single command
* Protocol compatibility in `posix` allows common access to files via posix or S3
* Simplified interface for adding new storage system support

### Lustre 向けバックエンド

Lustre のような並列ファイルシステム向けに `lustre` バックエンドを追加しました。アップロードごとに 1 本のスパースファイルを用意し、マルチパートの各 part を最終オブジェクト内で占める位置へ直接書き込みます。完了処理は `truncate` と `rename` だけで、データのコピーは発生しません。

その代わり part サイズを `--mpu-part-size` で固定する必要があり、これに従わない要求は拒否されます（[後述](#part-サイズは固定です)）。

> [!IMPORTANT]
> **バケットのバージョニングには非対応です。** part を最終位置へ直接書くと `posix` のバージョン管理機構が参照する part 単位のファイルが残らないためです。`--versioning-dir` を指定すると起動時にエラーになります。バージョニングが必要な場合は `--disable-direct-mpu` を指定して `posix` と同じコピー方式に切り替えてください。

**以下のような環境に最適です:**

* **reflink 非対応のファイルシステム** — `posix` バックエンドは各 part を個別の一時ファイルに書き、完了時にそれらを最終オブジェクトへコピーして結合します。このコピーが無償で済むのは reflink 対応 FS（XFS reflink、Btrfs、ZFS、ScoutFS）だけです。Lustre はファイルを別サーバ上の OST へストライプするためブロック共有が成立せず、全バイトがディスクへ 2 回書かれます
* **user xattr が使えないファイルシステム** — `--metadb` でオブジェクトとバケットの属性を SQLite データベースに格納するため、拡張属性に依存しません
* **メタデータサーバの負荷が課題になる環境** — 属性ごとに個別ファイルを作る `--sidecar` と違い 1 バケット = 1 ファイルなので、名前空間への問い合わせが大幅に減ります
* **5 GB を超える大きなオブジェクトを扱う環境** — 単発 PUT の上限を超えるためマルチパートが避けられないワークロード

#### 使い方

```
mkdir /tmp/vgw /tmp/vgwmeta
ROOT_ACCESS_KEY="testuser" ROOT_SECRET_KEY="secret" ./versitygw --port :10000 --iam-dir /tmp/vgw \
  lustre --metadb /tmp/vgwmeta --mpu-part-size 104857600 /tmp/vgw
```

主なオプション:

| オプション | 説明 |
|---|---|
| `--metadb <dir>` | 属性をバケット単位の SQLite データベースに格納する。オブジェクトデータとは別のファイルシステムに置ける。`posix` バックエンドでも利用可 |
| `--mpu-part-size <bytes>` | クライアントが送る part サイズ。**必須**（`--disable-direct-mpu` 指定時を除く） |
| `--disable-direct-mpu` | `posix` と同じコピー方式に戻す。バージョニングを使う場合に必要 |

#### part サイズは固定です

part N は `(N-1) × part サイズ` の位置に置かれるため、レイアウトが崩れる要求は暗黙にコピー方式へ退避させず、その場でエラーを返します。

| 状況 | 応答 |
|---|---|
| 起動時に `--mpu-part-size` 未指定 | 起動しない |
| 設定値より大きい part の `UploadPart` | `EntityTooLarge` (400) |
| 最終 part 以外が設定値と異なるサイズ | `CompleteMultipartUpload` で `InvalidRequest` (400) |
| part 番号が 1 から連番でない（歯抜けの subset で complete） | `CompleteMultipartUpload` で `InvalidRequest` (400) |
| アップロード進行中に別の `--mpu-part-size` で再起動 | 該当アップロードへの操作を `InvalidRequest` (400) で拒否 |

最終 part だけは設定値より短くて構いません。

そのため、本家 S3 では通れる「part ごとにサイズがばらばら」「アップロード済み part の一部だけを指定して complete」といった使い方は通りません。versitygw 付属の統合テスト 661 件では、5 MiB 設定時に part サイズが一致しない 3 件が失敗します（設定を合わせれば通ります）。既存のクライアントをそのまま繋ぐ用途ではなく、part サイズを固定できる移行パイプライン向けの設計です。

注意点:

* `--metadb` は sqlite3 の C ライブラリとリンクするため、ビルドに `CGO_ENABLED=1` が必要です
* `UploadPartCopy` はリクエストボディが無く直接スロットへ流し込めないため、コピー範囲を 1 回余分に読み書きします。バルク投入の経路ではないため許容しています

### WebGUI
Get more details about the new (optional) WebGUI management/explorer here: [https://github.com/versity/versitygw/wiki/WebGUI](https://github.com/versity/versitygw/wiki/WebGUI)

![admin-explorer](https://github.com/user-attachments/assets/e99db171-2c72-4d0f-8c8d-480a56e1c8a1)

### Static Website Hosting
Serve S3 buckets as static websites with index documents, custom error pages, and routing rules.
Enable a separate website endpoint with `--website :8090 --website-domain example.com` for virtual-host style routing (`blog.example.com` serves bucket `blog`, `example.com` serves bucket `example.com`).
When `--website-domain` is omitted, catch-all mode is used: the full hostname becomes the bucket name (name your buckets as FQDNs, e.g. `blog.example.com`).
See [Global Options](https://github.com/versity/versitygw/wiki/Global-Options) for all `--website-*` flags.

### News
Check out latest wiki articles: [https://github.com/versity/versitygw/wiki/Articles](https://github.com/versity/versitygw/wiki/Articles)

### Mailing List
Keep up to date with latest gateway announcements by signing up to the [versitygw mailing list](https://www.versity.com/products/versitygw#signup).

### Documentation
See project [documentation](https://github.com/versity/versitygw/wiki) on the wiki.

### Need help?
Ask questions in the [community discussions](https://github.com/versity/versitygw/discussions).
<br>
Contact [Versity Sales](https://www.versity.com/contact/) to discuss enterprise support.

### Overview
Versity Gateway, a simple to use tool for seamless inline translation between AWS S3 object commands and storage systems. The Versity Gateway bridges the gap between S3-reliant applications and other storage systems, enabling enhanced compatibility and integration while offering exceptional scalability.

The server translates incoming S3 API requests and transforms them into equivalent operations to the backend service. By leveraging this gateway server, applications can interact with the S3-compatible API on top of already existing storage systems. This project enables leveraging existing infrastructure investments while seamlessly integrating with S3-compatible systems, offering increased flexibility and compatibility in managing data storage.

The Versity Gateway is focused on performance, simplicity, and expandability. The Versity Gateway is designed with modularity in mind, enabling future extensions to support additional backend storage systems. At present, the Versity Gateway supports any generic POSIX file backend storage, Versity’s open source ScoutFS filesystem, Azure Blob Storage, and other S3 servers.

The gateway is completely stateless. Multiple Versity Gateway instances may be deployed in a cluster to increase aggregate throughput. The Versity Gateway’s stateless architecture allows any request to be serviced by any gateway thereby distributing workloads and enhancing performance. Load balancers may be used to evenly distribute requests across the cluster of gateways for optimal performance.

The S3 HTTP(S) server and routing is implemented using the [Fiber](https://gofiber.io) web framework.  This framework is actively developed with a focus on performance.  S3 API compatibility leverages the official [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2) whenever possible for maximum service compatibility with AWS S3.

## Getting Started
See the [Quickstart](https://github.com/versity/versitygw/wiki/Quickstart) documentation.

### Run the gateway with posix backend:

```
mkdir /tmp/vgw /tmp/vers
ROOT_ACCESS_KEY="testuser" ROOT_SECRET_KEY="secret" ./versitygw --port :10000 --iam-dir /tmp/vgw posix --versioning-dir /tmp/vers /tmp/vgw
```
This will enable an S3 server on the current host listening on port 10000 and hosting the directory `/tmp/vgw` with older object versions in `/tmp/vers`. It's fine if both of these directories are within the same filesystem. The `--iam-dir` option enables simple JSON flat file accounts for testing.

### Run the gateway with lustre backend:

```
mkdir /tmp/vgw /tmp/vgwmeta
ROOT_ACCESS_KEY="testuser" ROOT_SECRET_KEY="secret" ./versitygw --port :10000 --iam-dir /tmp/vgw lustre --metadb /tmp/vgwmeta /tmp/vgw
```
オプションの詳細と、このバックエンドが適した環境については上の [Lustre 向けバックエンド](#lustre-向けバックエンド) を参照してください。

To get the usage output, run the following:

```
./versitygw --help
```

The command format is

```
versitygw [global options] command [command options] [arguments...]
```
The [global options](https://github.com/versity/versitygw/wiki/Global-Options) are specified before the backend type and the backend options are specified after.

### Testing & Production Readiness

VersityGW is **battle-tested and production-ready**. Every pull request must pass our comprehensive test suite before it can be reviewed or merged. All code reviews are done by at least one human in the loop. LLMs may be used to augment the review process, but are never the sole reviewer or decision maker. See [Testing](https://github.com/versity/versitygw/wiki/Testing) for high level testing documentation.

#### Comprehensive Test Coverage

Our multi-layered testing strategy includes:

- **Go Unit Test Files** - Extensive unit tests with race detection and code coverage analysis covering core functionality, edge cases, and error handling.
- **Integration Test Scripts** - Real-world scenario testing across multiple backends (POSIX, S3, Azure) and configurations.
- **Functional/Regression Tests** - End-to-end SDK tests validating complete workflows including full-flow operations, POSIX-specific behavior, and IAM functionality populated with regression tests as issues are addressed.
- **Static Analysis** - Static Analysis checks using [staticcheck](https://staticcheck.dev).
- **System Tests** - Protocol-level validation using industry-standard S3 clients:
  - AWS CLI - Official AWS command-line tools
  - s3cmd - Popular S3 client
  - Direct REST API testing with curl for request/response validation
- **Security Testing** - Both HTTP and HTTPS configurations tested. Vulnerability scanning with govulncheck. And regular dependency updates with dependabot.
- **Compatibility Testing** - Multiple backends, versioning scenarios, static bucket modes, and various authentication methods.

### Run the gateway in Docker

Use the published image like the native binary by passing CLI arguments:

```bash
docker run --rm versity/versitygw:latest --version
```

See [Docker](https://github.com/versity/versitygw/wiki/Docker) for more
documentation for running within Docker.

### Run on Kubernetes

A Helm chart is provided to easily run Versity in Kubernetes environments:

```sh
helm install versitygw oci://ghcr.io/versity/versitygw/charts/versitygw
```

Please refer to the [chart's README](./chart/README.md) for more information and configuration parameters.

***

#### Versity gives you clarity and control over your archival storage, so you can allocate more resources to your core mission.

### Contact
![versity logo](https://www.versity.com/wp-content/uploads/2022/12/cropped-android-chrome-512x512-1-32x32.png)
info@versity.com <br />
+1 844 726 8826

### @versitysoftware
[![linkedin](https://github.com/versity/versitygw/blob/assets/assets/linkedin.jpg)](https://www.linkedin.com/company/versity/) &nbsp;
[![twitter](https://github.com/versity/versitygw/blob/assets/assets/twitter.jpg)](https://twitter.com/VersitySoftware) &nbsp;
[![facebook](https://github.com/versity/versitygw/blob/assets/assets/facebook.jpg)](https://www.facebook.com/versitysoftware) &nbsp;
[![instagram](https://github.com/versity/versitygw/blob/assets/assets/instagram.jpg)](https://www.instagram.com/versitysoftware/) &nbsp;
