# zenn-content

[Zenn](https://zenn.dev) で公開している記事のソースです。GitHub 連携により、`main` への push で同期されます。

データ基盤・分散処理まわりの記事を中心に置いています。
Hadoop / Spark / Flink / Kafka / ClickHouse を用いた大規模データ基盤の設計・構築・運用が専門です。

## 記事

| 記事 | 主なトピック |
|---|---|
| [Flink から MySQL への書き込みを最適化した話](articles/flink-mysql-write-reduction.md) | Flink SQL / 撤回ストリーム / タンブリングウィンドウ / JdbcSink |
| [Flink から Hive に書き込むときの小さなファイル問題を整理する](articles/flink-hive-small-files.md) | Flink / Hive / HDFS / sink.shuffle-by-partition.enable |
| [Spark SQL で Flink が Hive に書いた小さなファイルをマージする](articles/spark-sql-merge-small-files.md) | Spark 3 / REPARTITION_BY_RANGE / Tez との比較 |
| [Flink の監視とアラート設計について](articles/flink-monitoring-alerting.md) | Flink WebUI / バックプレッシャー / Kafka lag / YARN |
| [Flink の Broadcast State で新規ユーザーの行動をリアルタイム集計する](articles/flink-broadcast-state.md) | Broadcast State / Canal CDC / 状態の有効期限 |
| [Flink SQL で Hive テーブルをディメンションテーブルとして Join する](articles/flink-sql-hive-dim-join.md) | Temporal Table Join / lookup.join.cache.ttl / ORC |
| [10億件規模のアドホッククエリで Spark と Apache Kylin を実測比較する](articles/spark-vs-kylin-benchmark.md) | Spark / Apache Kylin / OLAP / 技術選定 |
| [Sqoop2 1.99.6 の本番運用で踏んだ不具合を、OSS のソースまで追って直した記録](articles/sqoop2-source-level-fixes.md) | Sqoop2 / JDBC 方言差 / 境界値バグ |

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
