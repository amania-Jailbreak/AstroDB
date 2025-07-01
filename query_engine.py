
class QueryEngine:
    """ドキュメントがクエリ条件に一致するかを判断するエンジン"""

    def matches(self, document: dict, query: dict) -> bool:
        """
        ドキュメントがクエリに完全に一致するかをチェックする。
        クエリ内のすべてのキーと値のペアがドキュメントに存在し、条件を満たす必要がある。
        """
        if not isinstance(query, dict):
            return False # クエリは辞書である必要がある

        for key, value in query.items():
            if key not in document:
                return False

            doc_value = document[key]

            if isinstance(value, dict):
                # 演算子 ($gt, $lt など) を含むクエリ
                for op, op_value in value.items():
                    if op == "$gt":
                        if not (isinstance(doc_value, (int, float)) and isinstance(op_value, (int, float)) and doc_value > op_value):
                            return False
                    elif op == "$lt":
                        if not (isinstance(doc_value, (int, float)) and isinstance(op_value, (int, float)) and doc_value < op_value):
                            return False
                    # 他の演算子 ($gte, $lte, $ne, $in, $nin など) はここに追加
                    else:
                        # 未知の演算子
                        return False
            else:
                # シンプルな値の一致
                if doc_value != value:
                    return False
        return True

# --- シングルトンインスタンス ---
query_engine_instance = QueryEngine()

if __name__ == '__main__':
    # --- モジュールの動作テスト ---
    print("--- クエリエンジンのテスト実行 ---")
    engine = QueryEngine()

    doc1 = {"title": "AstroDB", "author": "Amani", "year": 2025, "tags": ["db", "python"]}
    doc2 = {"title": "WebApp", "author": "Amani", "year": 2024, "tags": ["web", "js"]}
    doc3 = {"title": "AstroDB Guide", "author": "Gemini", "year": 2025, "tags": ["db", "guide"]}

    # 1. シンプルな一致テスト
    print("\n1. シンプルな一致テスト")
    query1 = {"author": "Amani"}
    assert engine.matches(doc1, query1) is True
    assert engine.matches(doc2, query1) is True
    assert engine.matches(doc3, query1) is False
    print("'author'フィールドでの検索が正常に機能。")

    # 2. 複数の条件での一致テスト
    print("\n2. 複数条件での一致テスト")
    query2 = {"author": "Amani", "year": 2025}
    assert engine.matches(doc1, query2) is True
    assert engine.matches(doc2, query2) is False # yearが違うのでFalse
    print("複数条件(AND)での検索が正常に機能。")

    # 3. 存在しないキーでのテスト
    print("\n3. 存在しないキーでのテスト")
    query3 = {"status": "published"}
    assert engine.matches(doc1, query3) is False
    print("存在しないキーでの検索が正常に機能。")

    # 4. 配列の一致テスト (完全一致のみ)
    print("\n4. 配列の完全一致テスト")
    query4 = {"tags": ["db", "python"]}
    assert engine.matches(doc1, query4) is True
    assert engine.matches(doc2, query4) is False
    print("配列の完全一致検索が正常に機能。")
    # 注: 現在の実装では 'tags'に'db'が含まれるか、という検索はできない。これは後ほど実装。

    # 5. $gt (より大きい) 演算子のテスト
    print("\n5. $gt (より大きい) 演算子のテスト")
    query_gt = {"year": {"$gt": 2024}}
    assert engine.matches(doc1, query_gt) is True
    assert engine.matches(doc2, query_gt) is False
    assert engine.matches(doc3, query_gt) is True
    print("$gt演算子での検索が正常に機能。")

    # 6. $lt (より小さい) 演算子のテスト
    print("\n6. $lt (より小さい) 演算子のテスト")
    query_lt = {"year": {"$lt": 2025}}
    assert engine.matches(doc1, query_lt) is False
    assert engine.matches(doc2, query_lt) is True
    assert engine.matches(doc3, query_lt) is False
    print("$lt演算子での検索が正常に機能。")

    # 7. $gt と $lt の組み合わせテスト
    print("\n7. $gt と $lt の組み合わせテスト")
    query_gt_lt = {"year": {"$gt": 2023, "$lt": 2025}}
    assert engine.matches(doc1, query_gt_lt) is False
    assert engine.matches(doc2, query_gt_lt) is True
    assert engine.matches(doc3, query_gt_lt) is False
    print("$gtと$ltの組み合わせ検索が正常に機能。")

    print("\nテスト成功！クエリエンジンが正常に機能しています。")
