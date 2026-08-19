---
title: "Flink から MySQL への書き込みを最適化した話"
emoji: "🌊"
type: "tech"
topics: ["flink", "mysql", "kafka", "sql", "データ基盤"]
published: false
---

:::message
本記事は、2021年に中国の技術ブログ CSDN で公開した記事を、筆者自身が日本語に書き直したものです。
原文：https://blog.csdn.net/xiaokan0230/article/details/115348328
:::

## 要件

5分ごとのアクティブユーザー数を集計して MySQL に書き込む、という要件です。
MySQL 側のデータ構造は以下のようになります。

| 時刻 | アクティブユーザー数 |
|---|---|
| 10:00 | 2 |
| 10:05 | 4 |
| 10:10 | 3 |

前置きは省いて、まず SQL から。

```sql
select
    p_product,
    p_project,
    p_dt,
    SUBSTRING(FROM_UNIXTIME(UNIX_TIMESTAMP(p_ts) / 300 * 300), 12, 5),
    count(distinct device_id)
from
    hive_catalog.t_kafka
    /*+ OPTIONS('connector.properties.group.id' = 'my_group') */
group by
    p_product,
    p_project,
    p_dt,
    SUBSTRING(FROM_UNIXTIME(UNIX_TIMESTAMP(p_ts) / 300 * 300), 12, 5)
```

`p_ts` がイベントタイムです。
`UNIX_TIMESTAMP(p_ts) / 300 * 300` で300秒（5分）単位に丸め、その文字列をキーにして数えています。

MySQL への書き込みコードは以下のとおりです。

```java
.addSink(JdbcSink.sink(
        "insert into ... values(?,?,?,?,?) on duplicate key update ...",
        (JdbcStatementBuilder<Row>) (preparedStatement, row) -> {
            preparedStatement.setString(1, getRowStr(row.getField(0)));
            preparedStatement.setString(2, getRowStr(row.getField(1)));
            preparedStatement.setString(3, getRowStr(row.getField(2)));
            preparedStatement.setString(4, getRowStr(row.getField(3)));
            preparedStatement.setInt(5, NumberUtils.createInteger(getRowStr(row.getField(4))));
        },
        new JdbcExecutionOptions.Builder()
                .withBatchSize(3000)
                .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName(parameterTool.getRequired("mysql.driver.name"))
                .withUrl(parameterTool.getRequired("mysql.url"))
                .withUsername(parameterTool.getRequired("mysql.username"))
                .withPassword(parameterTool.getRequired("mysql.password"))
                .build()));
```

## リリース後の書き込み速度を観測する

![Flink WebUI の Metrics タブ。3つの Sink サブタスクがそれぞれ 2358/s、126/s、370/s を示している](/images/55e9e11a3e7c671f6e6b1664180762e2.png)

バックプレッシャーもなく、すべて正常です。MySQL への書き込み速度は毎秒2,900件前後でした。

次に、Kafka の消費方針を `from-beginning` に設定して、Flink 側の消費限界を測ります。

![Flink WebUI の BackPressure タブ。Flat Map オペレータの Ratio が 1、Status が HIGH になっている](/images/66dd3306553cf70f669bd2b891ff2f0e.png)

深刻なバックプレッシャーを起こした状態で、MySQL への書き込み速度は毎秒約1万件でした。
通常時の毎秒2,900件を大きく上回っています。

というわけで、リリースして完了。

## 万事うまくいったと思っていたら、2回落ちた

ところがリリース後、Flink ジョブが先後2回停止しました。
ログを確認すると MySQL の応答タイムアウトです。

調べてみると、オフライン処理のチームが毎晩定時に同じ MySQL へ大量のデータを流し込んでおり、
その間 MySQL の負荷が非常に高くなって、リアルタイム側のリソースを奪っていたことが分かりました。

バッチ側での対応も相談しましたが、原因の見立てが分かれ、すぐに改修に入れる状況ではありませんでした。
他人は変えられませんが、自分は変えられます。

Flink 側から MySQL への負荷と依存を下げるため、以下のように改修しました。

```java
// 元の集計クエリ（撤回ストリームを出す）
Table table = bsEnv.sqlQuery(
        "select p_product,p_project,p_dt,SUBSTRING(FROM_UNIXTIME(UNIX_TIMESTAMP(p_ts) ... " +
        " group by p_product,p_project,p_dt,SUBSTRING(FROM_UNIXTIME(UNIX_TIMESTAMP(p_ts) ... ");
table.printSchema();

// kafka のフィールド情報
TypeInformation[] typeInfos = {
        Types.STRING,
        Types.STRING,
        Types.STRING,
        Types.STRING,
        Types.LONG
};

// 撤回ストリームから、撤回行を落として更新行だけを残す
DataStream<Row> res = bsEnv.toRetractStream(table, Row.class)
        .flatMap(new FlatMapFunction<Tuple2<Boolean, Row>, Row>() {
            @Override
            public void flatMap(Tuple2<Boolean, Row> value, Collector<Row> out) throws Exception {
                if (value.f0) {
                    out.collect(value.f1);
                }
            }
        }).returns(new RowTypeInfo(typeInfos, typeNames));

// 1秒のタンブリングウィンドウをかけ直す
bsEnv.createTemporaryView("t", res);
Table t = bsEnv.sqlQuery("select ... from t group by ..., TUMBLE(proctime(), INTERVAL '1' SECONDS)");

bsEnv.toAppendStream(t, Row.class)
        .addSink(JdbcSink.sink(
                "insert into ... values(?,?,?,?,?) on duplicate key update ...",
                /* ... 中略 ... */
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(20)
                        .build(),
                /* ... */));
```

ここで主に説明しておきたい点が3つあります。

**1. なぜ SQL を2つに分けたのか。元の SQL にウィンドウ構文をかぶせないのはなぜか。**

元のクエリが撤回ストリームであるため、Flink はその書き方をサポートしておらず、エラーになります。

**2. なぜ元の `group by` に `TUMBLE(proctime(), INTERVAL '1' SECONDS)` を直接足さないのか。**

意味が変わってしまうためです。
集計対象がウィンドウ内の5分間のデータだけになり、時刻キーで束ねた履歴全体ではなくなります。
遅れて到着したデータが加算できず、値が正しくなくなります。

**3. なぜ FlatMap を一段挟むのか。**

撤回ストリームから撤回された行を取り除き、更新行だけを残すためです。

## 効果を見る

![改修後の Flink WebUI の Metrics タブ。3つの Sink サブタスクが 4/s、3/s、4/s を示しており、オペレータに GroupWindowAggregate が追加されている](/images/f31c4ad4e6e73a9819866c3e90b309f7.png)

1秒のウィンドウを入れても、ユーザー体験は変わりません。なぜそう言えるのか。

元の毎秒2,900件のときは、MySQL への書き込みバッチを3,000に設定していたので、
MySQL のデータはおおむね1秒に1回更新されていました。

1秒ウィンドウに変更した後は、平均の書き込み量が毎秒20件程度になるため、
書き込みバッチを20に変更しています。こうすると MySQL のデータはやはり1秒に1回更新されます。

つまりユーザーから見た体験は変わらないまま、MySQL の負荷だけが100分の1以上に下がったことになります。

この最適化以降、MySQL の接続が切れる事象は一度も発生していません。

繰り返しになりますが、他人は変えられません。しかし自分は変えられます。

---

原文（中国語）：https://blog.csdn.net/xiaokan0230/article/details/115348328
