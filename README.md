# zenn-content

[Zenn](https://zenn.dev) で公開している記事のソースです。GitHub 連携により、`main` への push で同期されます。

データ基盤・分散処理まわりの記事を中心に置いています。
Hadoop / Spark / Flink / Kafka / ClickHouse を用いた大規模データ基盤の設計・構築・運用が専門です。

## 記事

| 記事 | 主なトピック |
|---|---|
| [Flink から MySQL への書き込みを最適化した話](articles/flink-mysql-write-reduction.md) | Flink SQL / 撤回ストリーム / タンブリングウィンドウ / JdbcSink |

2016年から2023年にかけて中国の技術ブログ（CSDN・51CTO）で発信した記事のうち、
現在も有用と思われるものを筆者自身が日本語に書き直して掲載しています。
各記事の冒頭に原文の URL を明記しています。

- CSDN: https://blog.csdn.net/xiaokan0230
- 51CTO: https://blog.51cto.com/xk0230

## 構成

```
articles/   記事の Markdown
images/     記事内で参照する画像（/images/... の絶対パスで参照する）
```

## ローカルでの確認

```bash
npm install
npx zenn preview   # http://localhost:8000
```
