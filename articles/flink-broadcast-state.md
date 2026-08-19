---
title: "Flink の Broadcast State で新規ユーザーの行動をリアルタイム集計する"
emoji: "📻"
type: "tech"
topics: ["flink", "kafka", "mysql", "canal", "データ基盤"]
published: false
---

:::message
本記事は、2021年に中国の技術ブログ CSDN で公開した記事を、筆者自身が日本語に書き直したものです。
原文：https://blog.csdn.net/xiaokan0230/article/details/114984133
:::

## 要件

- ユーザーの行動データはすべて Kafka にある
- ユーザーマスタは MySQL にあり、新規ユーザーが来ると1レコード追加される
- **新規ユーザーの行動だけ**をリアルタイムに集計し、下流の Kafka へ書き込みたい

## 方針

Flink の Broadcast State を使います。

公式ドキュメントの例では、1つ目のストリームが `Color` と `Shape` を属性に持つ `Item` 型の要素、
もう1つのストリームが `Rules` を含む、という構成になっています。

![Flink 公式ドキュメントの Broadcast State の図](/images/b54c3b2f11823c21831b31c043fbbc36.png)

公式の例に当てはめると、**Item ストリームが我々のユーザー行動、Rules ストリームが MySQL のユーザーテーブル**に相当します。
以降ではそれぞれ「データストリーム」「ブロードキャストストリーム」と呼びます。

ここでは **Canal を使って MySQL の binlog を Kafka のストリームデータへ変換**します。
これでブロードキャストストリームが手に入ります。

```java
env.addSource(...)
   .name("mysql").uid("mysql")
   .flatMap(new FlatMapFunction<JsonNode, Tuple6<...>>() {
       @Override
       public void flatMap(JsonNode value, Collector<Tuple6<...>> out) {
           // Canal の JSON から type を見て INSERT だけを拾う
           if ("INSERT".equals(v.get("type").asText())) {
               ...
           }
       }
   });
```

2つのストリームを `connect` でつなぎ、`process` メソッドに具体的な処理ロジックを実装します。
`process` では `BroadcastProcessFunction` と `KeyedBroadcastProcessFunction` の2通りが使えます。

```java
MapStateDescriptor<String, Tuple6<...>> descriptor = new MapStateDescriptor<>(
        "RulesBroadcastState",
        BasicTypeInfo.STRING_TYPE_INFO,
        TypeInformation.of(new TypeHint<Tuple6<...>>() {}));

// MySQL テーブルと関連づける。最新ユーザーのみを対象にする
resultDataStream.connect(broadcastStream.broadcast(descriptor))
                .process(new UserBroadcastProcessFunction())
                ...;
```

今回は `BroadcastProcessFunction` を選びました。

![BroadcastProcessFunction が実装すべき2つのメソッド](/images/e96f002ba2f06fa0a24424b1ab7755ff.png)

`BroadcastProcessFunction` では上記の2つのメソッドを実装する必要があります。

- `processElement` … ユーザー行動データの処理ロジック、つまりデータ側のロジック
- `processBroadcastElement` … MySQL binlog ストリームの処理ロジック、つまりブロードキャスト側のロジック

## ここで強調しておきたい点

**`processElement` から取得できるブロードキャスト変数は読み取り専用です。**

つまりデータストリーム側からブロードキャスト変数を変更することはできません。
データストリームに特定のデータが現れたときにブロードキャスト変数を追加・削除する、
といったことはできない、ということです。

**ブロードキャスト変数の追加・削除・変更は `processBroadcastElement` でしか実装できません。**
今回の要件で言えば、MySQL に新しいユーザーが来たときにだけ、
ブロードキャスト変数を追加・変更・削除できる、ということになります。

## 公式の例が触れていない問題：ブロードキャスト変数の無限膨張をどう防ぐか

Flink SQL であれば統一的な状態の有効期限を設定して状態の無限膨張を避けられますが、
ここでは **`processBroadcastElement` の中で無効な状態を自分で掃除する**必要があります。

たとえば「12時間以内の新規ユーザーの行動を処理する」という前提なら、
12時間を超えた新規ユーザーのデータはブロードキャスト変数から取り除きます。

![processBroadcastElement 内で期限切れの状態を削除している実装](/images/695643e8f007f5881754f44c701ffc42.png)

ここでは**現在時刻を比較に使っています**。
`processBroadcastElement` の処理ロジックはデータストリームとは無関係なので、
データストリーム側の時刻を基準にして追加・削除を判断することができないためです。

![processElement で読み取り専用のブロードキャスト状態を取得している実装](/images/3ca511def4577570cd4656d900091e1c.png)

上図のように、`processElement` でデータストリームを処理する際は、
読み取り専用のブロードキャスト状態を取得して業務ロジックを実装します。

## Broadcast State を使うときの注意点

公式ドキュメントに記載されている前提を、実装者の視点で押さえておきます。

**タスク間の通信がない**
これが、ブロードキャスト側（`(Keyed)-BroadcastProcessFunction`）だけが
ブロードキャスト状態を変更できる理由です。
さらに利用者は、**すべてのタスクが各要素に対して同じようにブロードキャスト状態を変更する**ことを
保証しなければなりません。そうでないとタスクごとに内容が食い違い、結果が一致しなくなります。

**ブロードキャスト状態におけるイベントの順序はタスクごとに異なりうる**
ブロードキャストストリームの要素は最終的にすべての下流タスクへ届くことは保証されますが、
**到達する順序はタスクごとに異なる可能性があります**。
したがって各要素に対する状態更新は、到着順に依存してはいけません。

**すべてのタスクがブロードキャスト状態をチェックポイントする**
チェックポイント実行時、すべてのタスクは同じ要素を持ちますが、
**1つだけでなく全タスクがブロードキャスト状態をチェックポイントします**。
これは復元時に全タスクが同一ファイルを読みに行くこと（ホットスポット）を避けるための設計判断で、
代償としてチェックポイント状態のサイズが並列度 p 倍になります。
Flink は復元・スケール後にデータの重複も欠落も起きないことを保証します。

## まとめ

1. ブロードキャスト変数を変更できるのは `processBroadcastElement` のみ。`processElement` からは読み取り専用
2. 状態の更新はブロードキャストストリームの到着順に依存させない
3. **状態の有効期限を設定し、状態が無限に膨張しないようにする**
4. **ブロードキャスト変数は大きすぎないこと。** 実行時はメモリ上に保持されるため
5. 起動時の順序の乱れによるデータ不正確に注意する

---

原文（中国語）：https://blog.csdn.net/xiaokan0230/article/details/114984133
