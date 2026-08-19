---
title: "Flink SQL で Hive テーブルをディメンションテーブルとして Join する"
emoji: "🔗"
type: "tech"
topics: ["flink", "hive", "kafka", "sql", "データ基盤"]
published: false
---

:::message
本記事は、2021年に中国の技術ブログ CSDN で公開した記事を、筆者自身が日本語に書き直したものです。
原文：https://blog.csdn.net/xiaokan0230/article/details/114664873
:::

## 前提

Flink 1.11 から Hive テーブルとの Join がサポートされました。公式ドキュメントは以下のとおりです。

![Flink 1.11 公式ドキュメントの Hive ディメンションテーブル Join に関する記述](/images/7094b8a756fbbeadb5bcc02cf660249b.png)

公式ドキュメントによると、

1. **Hive テーブルは TaskManager のメモリにキャッシュされる**ので、Join する Hive テーブルは大きすぎないほうがよい
2. `lookup.join.cache.ttl` パラメータに従って、Flink は定期的に Hive のキャッシュを更新する

一方こちらの業務要件は、**Flink が Kafka と T+1 で更新される Hive テーブルを読み、
Kafka のデータのうち type フィールドが Hive テーブルに存在するものだけを処理する**というものでした。

## まず試して駄目だった書き方

最初に試したのはこの SQL です。

```sql
select a.* from flink_tab a where a.type in (select type from hive_tab);
```

この SQL は動きますし、Hive テーブルも読みに行きます。
しかし **Flink Web UI で見ると、Hive テーブルを読み終わった時点で task が finish してしまいます。**
Hive のデータを定期的に更新することもありません。これでは要件を満たせません。

## Flink 1.12 の標準的な書き方

Flink 1.12 のドキュメントには標準的な書き方が示されています。

![Flink 1.12 公式ドキュメントのディメンションテーブル Join の記述](/images/ecce26e75dc865e68ff34759e5252ad9.png)

主に効いてくるパラメータは3つです。

### `streaming-source.enable`

デフォルト `false`。ストリーミングソースとして扱うかどうかを決めます。
ドキュメントには、**各パーティション／ファイルがアトミックに書き込まれることを保証すること**、
さもないとリーダーが不完全なデータを読む可能性がある、と注意書きがあります。

今回は **Hive を bounded stream として扱いたい**ので、ここは `false` を選びます。

### `streaming-source.partition.include`

デフォルト `all`。読み取り対象のパーティションを指定します。`all` と `latest` が使えます。

**このパラメータは Flink 1.11 ではサポートされておらず、1.12 で入りました。**

ここは Hive テーブルの設計に関わってきます。

- `all` にするなら、**Hive テーブルには現時点で有効な設定だけを保持する**必要があります
- `latest` にするなら、最新パーティションだけを読ませる運用にできます

### `lookup.join.cache.ttl`

キャッシュの更新間隔です。このパラメータが**実際に効いていること**と、
**正しいデータを読めていること**を確認する必要があります。

## パラメータが効いていることを確認する

まず Hive テーブル側にパラメータを設定します。

```sql
alter table hive_tab set tblproperties('lookup.join.cache.ttl' = '10 min');
```

![設定前のリフレッシュ間隔がログに出ている画面](/images/06f9e6bd41a3566d8abb6f8f7f41e799.png)

次に値を変えて、ジョブを起動し直します。

```sql
alter table hive_tab set tblproperties('lookup.join.cache.ttl' = '1 min');
```

![リフレッシュ間隔が 1min に変わったことがログで確認できる画面](/images/60bc659365231cb54f9647c6b6075c65.png)

リフレッシュ頻度が 1min に変わったことが確認できます。

## ログから分かった重要なこと

ログに出ている **`Reading ORC rows from ***`** から、
**Flink は Hive を読むとき、Hive JDBC 経由でデータを受け取るのではなく、
ORC ファイルを直接読んでいる**ことが分かります。

この事実は、**なぜ Flink が Hive テーブルを Join する際、
Join 対象が Hive のサブクエリだとエラーになるのか**を説明してくれます。
ファイルを直接読んでいるので、そもそもクエリを評価する経路が無いわけです。

### ここから導かれる設計指針

Join する Hive テーブルのデータに加工が必要な場合、
たとえば `hive a join hive b` して `hive c` を生成し、その `hive c` を Flink と Join したい場合は、

**オフラインの T+1 処理であらかじめ `hive c` を作っておき、
Flink は `hive c` とだけやり取りする**

という構成にすることを勧めます。

## 最終的な SQL

```sql
select a.f0, a.f1, a.f2 from
    (select f0, f1, f2, PROCTIME() as proctime from flink_tab) as a
    inner join hive_tab FOR SYSTEM_TIME AS OF a.proctime as v
    on a.type = v.type
```

`FOR SYSTEM_TIME AS OF` を使った Temporal Table Join の形にすることで、
処理時刻時点の Hive スナップショットと結合でき、
かつ `lookup.join.cache.ttl` に従ってキャッシュが更新され続けます。

---

原文（中国語）：https://blog.csdn.net/xiaokan0230/article/details/114664873
