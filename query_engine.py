from typing import Any
import logging
import re

# ロガーの設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class QueryEngine:
    """ドキュメントがクエリ条件に一致するかを判断するエンジン"""

    def _get_nested_value(self, document: dict, key_path: str):
        """
        ドット記法で指定されたパスに基づいて、ネストされたドキュメントから値を取得する。
        例: "user.profile.age"
        """
        keys = key_path.split(".")
        current_value = document
        for key in keys:
            if isinstance(current_value, dict) and key in current_value:
                current_value = current_value[key]
            else:
                return None  # パスが見つからない場合
        return current_value

    def _match_field(self, doc_value: Any, query_value: Any) -> bool:
        """
        単一のドキュメントフィールドがクエリ値に一致するかをチェックするヘルパーメソッド。
        """
        if isinstance(query_value, dict):
            # 演算子 ($gt, $lt など) を含むクエリ
            for op, op_value in query_value.items():
                if op == "$gt":
                    if not (
                        isinstance(doc_value, (int, float))
                        and isinstance(op_value, (int, float))
                        and doc_value > op_value
                    ):
                        return False
                elif op == "$lt":
                    if not (
                        isinstance(doc_value, (int, float))
                        and isinstance(op_value, (int, float))
                        and doc_value < op_value
                    ):
                        return False
                elif op == "$gte":
                    if not (
                        isinstance(doc_value, (int, float))
                        and isinstance(op_value, (int, float))
                        and doc_value >= op_value
                    ):
                        return False
                elif op == "$lte":
                    if not (
                        isinstance(doc_value, (int, float))
                        and isinstance(op_value, (int, float))
                        and doc_value <= op_value
                    ):
                        return False
                elif op == "$ne":
                    if doc_value == op_value:
                        return False
                elif op == "$in":
                    if not isinstance(op_value, list):
                        return False  # $in の値はリストである必要がある
                    if isinstance(doc_value, list):
                        # ドキュメントの値がリストの場合、クエリのいずれかの値が含まれているか
                        if not any(item in op_value for item in doc_value):
                            return False
                    elif doc_value not in op_value:
                        return False
                elif op == "$nin":
                    if not isinstance(op_value, list):
                        return False  # $nin の値はリストである必要がある
                    if isinstance(doc_value, list):
                        if any(item in op_value for item in doc_value):
                            return False
                    elif doc_value in op_value:
                        return False
                elif op == "$all":  # 配列内の全要素が一致
                    if not isinstance(doc_value, list) or not isinstance(
                        op_value, list
                    ):
                        return False
                    if not all(item in doc_value for item in op_value):
                        return False
                elif op == "$elemMatch":  # 配列内の要素が指定されたクエリに一致
                    if not isinstance(doc_value, list) or not isinstance(
                        op_value, dict
                    ):
                        return False
                    if not any(self.matches(item, op_value) for item in doc_value):
                        return False
                elif op == "$regex":  # 正規表現マッチ
                    if not isinstance(doc_value, str) or not isinstance(op_value, str):
                        return False
                    if not re.search(op_value, doc_value):
                        return False
                elif op == "$size":  # 配列のサイズ
                    if not isinstance(doc_value, list) or not isinstance(op_value, int):
                        return False
                    if len(doc_value) != op_value:
                        return False
                elif op == "$exists":  # フィールドの存在チェック
                    if not isinstance(op_value, bool):
                        return False
                    if op_value and doc_value is None:
                        return False
                    if not op_value and doc_value is not None:
                        return False
                else:
                    # 未知の演算子
                    return False
        else:
            # シンプルな値の一致
            if isinstance(doc_value, list) and isinstance(query_value, list):
                # 両方がリストの場合、完全一致をチェック
                if doc_value != query_value:
                    return False
            elif isinstance(doc_value, list):
                # doc_valueがリストで、query_valueが単一の値の場合、query_valueがdoc_valueに含まれているかチェック
                if query_value not in doc_value:
                    return False
            elif doc_value != query_value:
                # それ以外の場合、単純な値の一致をチェック
                return False
        return True

    def matches(self, document: dict, query: dict) -> bool:
        """
        ドキュメントがクエリに完全に一致するかをチェックする。
        クエリ内のすべてのキーと値のペアがドキュメントに存在し、条件を満たす必要がある。
        """
        if not isinstance(query, dict):
            return False  # クエリは辞書である必要がある

        if "$and" in query:
            # $and 演算子: すべてのサブクエリがTrueである必要がある
            for sub_query in query["$and"]:
                if not self.matches(document, sub_query):
                    return False
            return True

        if "$or" in query:
            # $or 演算子: いずれかのサブクエリがTrueである必要がある
            for sub_query in query["$or"]:
                if self.matches(document, sub_query):
                    return True
            return False

        for key, value in query.items():
            # ドット記法を処理
            doc_value = self._get_nested_value(document, key)

            if (
                doc_value is None and key not in document
            ):  # ネストされたパスが見つからない、かつトップレベルにもキーがない
                return False

            if not self._match_field(doc_value, value):
                return False
        return True


# --- シングルトンインスタンス ---
query_engine_instance = QueryEngine()
