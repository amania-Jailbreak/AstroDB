
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
                    elif op == "$gte":
                        if not (isinstance(doc_value, (int, float)) and isinstance(op_value, (int, float)) and doc_value >= op_value):
                            return False
                    elif op == "$lte":
                        if not (isinstance(doc_value, (int, float)) and isinstance(op_value, (int, float)) and doc_value <= op_value):
                            return False
                    elif op == "$ne":
                        if doc_value == op_value:
                            return False
                    elif op == "$in":
                        if not isinstance(op_value, list):
                            return False # $in の値はリストである必要がある
                        if isinstance(doc_value, list):
                            if not any(item in op_value for item in doc_value):
                                return False
                        elif doc_value not in op_value:
                            return False
                    elif op == "$nin":
                        if not isinstance(op_value, list):
                            return False # $nin の値はリストである必要がある
                        if isinstance(doc_value, list):
                            if any(item in op_value for item in doc_value):
                                return False
                        elif doc_value in op_value:
                            return False
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
    doc4 = {"title": "Another DB", "author": "Bob", "year": 2023, "tags": ["db", "sql"]}

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

    # 7. $gte (以上) 演算子のテスト
    print("\n7. $gte (以上) 演算子のテスト")
    query_gte = {"year": {"$gte": 2025}}
    assert engine.matches(doc1, query_gte) is True
    assert engine.matches(doc2, query_gte) is False
    assert engine.matches(doc3, query_gte) is True
    print("$gte演算子での検索が正常に機能。")

    # 8. $lte (以下) 演算子のテスト
    print("\n8. $lte (以下) 演算子のテスト")
    query_lte = {"year": {"$lte": 2024}}
    assert engine.matches(doc1, query_lte) is False
    assert engine.matches(doc2, query_lte) is True
    assert engine.matches(doc3, query_lte) is False
    print("$lte演算子での検索が正常に機能。")

    # 9. $ne (等しくない) 演算子のテスト
    print("\n9. $ne (等しくない) 演算子のテスト")
    query_ne = {"author": {"$ne": "Amani"}}
    assert engine.matches(doc1, query_ne) is False
    assert engine.matches(doc2, query_ne) is False
    assert engine.matches(doc3, query_ne) is True
    print("$ne演算子での検索が正常に機能。")

    # 10. $in (配列に含まれる) 演算子のテスト
    print("\n10. $in (配列に含まれる) 演算子のテスト")
    query_in = {"tags": {"$in": ["js", "sql"]}}
    assert engine.matches(doc1, query_in) is False
    assert engine.matches(doc2, query_in) is True
    assert engine.matches(doc3, query_in) is False
    assert engine.matches(doc4, query_in) is True
    print("$in演算子での検索が正常に機能。")

    # 11. $nin (配列に含まれない) 演算子のテスト
    print("\n11. $nin (配列に含まれない) 演算子のテスト")
    query_nin = {"tags": {"$nin": ["js", "sql"]}}
    assert engine.matches(doc1, query_nin) is True
    assert engine.matches(doc2, query_nin) is False
    assert engine.matches(doc3, query_nin) is True
    assert engine.matches(doc4, query_nin) is False
    print("$nin演算子での検索が正常に機能。")

    print("\nテスト成功！クエリエンジンが正常に機能しています。")
