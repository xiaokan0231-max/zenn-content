---
title: "Spark SQL で Flink が Hive に書いた小さなファイルをマージする"
emoji: "🧹"
type: "tech"
topics: ["spark", "hive", "flink", "hadoop", "データ基盤"]
published: false
---

:::message
本記事は、2021年に中国の技術ブログ CSDN で公開した記事を、筆者自身が日本語に書き直したものです。
原文：https://blog.csdn.net/xiaokan0230/article/details/114676638
:::

## 1. 背景

Flink 1.11 が Hive への直接書き込みをサポートしたことで、ストリームとバッチの統合がさらに進みました。
ただ `sink.shuffle-by-partition.enable` と checkpoint 間隔の調整で
Flink が生む小さなファイルをできる限り減らしても、
**Flink 1.12 の自動マージ機能をもってしても、小さなファイルの発生を完全には避けられません。**

:::message
この点については別記事「[Flink から Hive に書き込むときの小さなファイル問題を整理する](https://zenn.dev/xk0230/articles/flink-hive-small-files)」で詳しく扱っています。本記事はその続きにあたります。
:::

そのため、Flink が Hive に書いた小さなファイルを**定期的にマージする**必要があります。

## 2. Hive Tez でマージする方法

まず Tez で試しました。

```sql
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=...;
set hive.exec.max.dynamic.partitions.pernode=...;
set hive.merge.smallfiles.avgsize=...;
set hive.merge.size.per.task=...;

insert overwrite table t partition (p_dt, p_hours)
select * from t where ...;
```

### Tez によるマージの問題点

1. **マージ時に大量の YARN メモリを消費します。**
   2〜3日分のデータをマージするだけで **TB 級のメモリ**を占有し、
   メモリ使用量がまったく制御できず、他の業務クラスタの安定性に影響が出ました。
2. 上記の理由から**一度にマージできるのは1〜2日分**が限界です。
   数ヶ月分をマージしたければスクリプトで繰り返し実行することになりますが、
   タスクの起動が頻繁になるため総所要時間が非常に長くなり、クラスタへの負荷も大きく、
   安定性の面で不利です。

## 3. Spark SQL でマージする方法

```sql
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.adaptive.enabled=true;
set spark.sql.adaptive.coalescePartitions.enabled=true;
set spark.shuffle.sort.bypassMergeThreshold=...;
set spark.sql.shuffle.partitions=...;
```

### 対象のファイル数を見積もる

Flink が Hive に生成するファイルは以下のようになります。
ここで Flink の checkpoint は5分です。

理論上は **(12 + n + 1) × 24 個**のファイルができます。
n は時間をまたいで遅れて到着したデータのファイル数です。
たとえば10時01分に9時のデータが来る、といったケースで、
**プロセスタイムではなくデータの時刻でパーティションを切っている**ため発生します。

実際にはおおむね **300個以上**になります。

```bash
hdfs dfs -count -h /user/hive/warehouse/<db>.db/<table>/*
```

![hdfs dfs -count でパーティションごとのファイル数を確認した結果](/images/e399b27de0554c7398de8dd4c43050a4.jpeg)

### Spark 3.0 を使う理由：ヒント句

Spark 3.0 を使う理由は **hint（ヒント句）**にあります。
これがあると SQL だけで小さなファイルをマージできます。

公式ドキュメントによると、

- **COALESCE** … パーティション数を指定した数まで減らす。パラメータとしてパーティション数を取る
- **REPARTITION** … 指定した数でパーティションを切り直す

ここでは **`REPARTITION_BY_RANGE`** を選びました。

**ヒント句を付けなくても Spark はある程度は小さなファイルをマージしますし、
そのほうが性能もかなり速くなります。ただし完全にマージされることは保証されません。**
ヒント句を付ければ完全にマージされることを保証できますが、その分だけ時間がかかります。

### 実行

```bash
spark-sql --master yarn --queue common \
  --num-executors 10 --executor-cores 8 --executor-memory 28G
```

**core は多くしすぎないほうがよいです。** 多すぎると OOM を起こす可能性があります。

ヒント句なしの場合：

```sql
insert overwrite table t partition (p_dt, p_hours)
select * from t where p_dt between '2021-01-20' and '2021-02-19';
```

![ヒント句なしで実行した場合のファイル数](/images/70b8408ca5f769b1226df79f4dbae876.jpeg)

ヒント句ありの場合：

```sql
insert overwrite table t partition (p_dt, p_hours)
select /*+ REPARTITION_BY_RANGE(p_dt, p_hours) */ *
from t where p_dt between '2021-01-20' and '2021-02-19';
```

![ヒント句ありで実行した場合、ファイル数がヒント句どおりに揃っている](/images/b57c8f83ee042c6e93ba7c3463d7765f.jpeg)

**ヒント句を付けると、データファイル数が厳密にヒント句のとおりになります。**

## 4. Tez と比べて何が良くなったか

1. **同じデータに対して、300G のメモリで数ヶ月分を一度に処理できます。**
   タスクを頻繁に起動する必要がなくなり、時間が大幅に削減され、
   総マージ時間は Tez 方式よりはるかに短くなりました
2. **メモリ使用量が 300G で安定して制御でき**、他のプログラムの動作に影響しません
3. **Spark はデフォルトで snappy 圧縮を行います。**
   Tez のマージでは圧縮されません。圧縮後のファイルサイズはおよそ**元の4分の1**になり、
   **ストレージコストが75%削減**されました
4. Spark のスクリプトでマージする方式と比べても、Spark SQL でマージするほうが手軽です。
   **既存の Spark SQL で書かれたオフラインタスクをそのまま改造できる**ので、
   一部のオフラインタスクについては小さなファイル問題を根本から解消できます

---

原文（中国語）：https://blog.csdn.net/xiaokan0230/article/details/114676638
