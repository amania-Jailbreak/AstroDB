import ujson
from threading import Lock
from pathlib import Path
import uuid
import logging

# ロガーの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# プロジェクトのモジュールをインポート
import encryption
import query_engine

# --- 定数 ---
DATABASE_FILE = Path("database.adb")


class AstroDB:
    def __init__(self):
        self._db = {
            "collections": {},
            "_index_definitions": {},
        }  # データストア本体例: {"collections": {"users": [...]}, "_index_definitions": {"users": ["name"]}}
        self._indexes = (
            {}
        )  # インデックスデータ例: {"users": {"email": {"a@b.com": doc_id}}}
        self._lock = Lock()  # スレッドセーフのためのロック
        self.load_from_disk()

    def load_from_disk(self):
        """暗号化されたデータベースファイルをディスクから読み込む"""
        with self._lock:
            if not DATABASE_FILE.exists():
                logger.info(
                    "データベースファイルが存在しないため、新しいデータベースを作成します。"
                )
                self._db = {
                    "collections": {},
                    "_index_definitions": {},
                }  # _index_definitionsも初期化
                return

            try:
                with open(DATABASE_FILE, "rb") as f:
                    encrypted_data = f.read()

                if not encrypted_data:
                    logger.warning(
                        "データベースファイルが空です。新しいデータベースを作成します。"
                    )
                    self._db = {
                        "collections": {},
                        "_index_definitions": {},
                    }  # _index_definitionsも初期化
                    return

                decrypted_json = encryption.decrypt(encrypted_data)
                self._db = ujson.loads(decrypted_json)
                # 互換性のため、古いDB形式の場合に_index_definitionsを追加
                if "_index_definitions" not in self._db:
                    self._db["_index_definitions"] = {}
                logger.info("データベースをディスクから正常に読み込みました。")
                self._rebuild_indexes()
            except Exception as e:
                logger.critical(f"致命的エラー: データベースの読み込みに失敗しました。 {e}")
                # ファイルが破損している可能性などを考慮し、空のDBで起動
                self._db = {"collections": {}, "_index_definitions": {}}

    def save_to_disk(self):
        """メモリ上のデータベースを暗号化してディスクに保存する"""
        with self._lock:
            try:
                db_json = ujson.dumps(self._db)
                encrypted_data = encryption.encrypt(db_json)
                with open(DATABASE_FILE, "wb") as f:
                    f.write(encrypted_data)
                logger.info("データベースをディスクに正常に保存しました。")
            except Exception as e:
                logger.error(f"エラー: データベースの保存に失敗しました。 {e}")

    def _rebuild_indexes(self):
        """データベースのロード時にインデックスを再構築する内部メソッド"""
        self._indexes = {}
        for collection_name, documents in self._db["collections"].items():
            if collection_name not in self._indexes:
                self._indexes[collection_name] = {}

            # _idは常にインデックスされると仮定
            if "_id" not in self._indexes[collection_name]:
                self._indexes[collection_name]["_id"] = {}
            for doc in documents:
                if "_id" in doc:
                    self._indexes[collection_name]["_id"][doc["_id"]] = doc

            # 永続化されたインデックス定義に基づいてインデックスを再構築
            if collection_name in self._db["_index_definitions"]:
                for field in self._db["_index_definitions"][collection_name]:
                    if field not in self._indexes[collection_name]:
                        self._indexes[collection_name][field] = {}
                    for doc in documents:
                        if field in doc:
                            self._indexes[collection_name][field][doc[field]] = doc

    # --- データ操作API ---

    def insert_one(self, collection_name: str, document: dict, owner_id: str) -> dict:
        """
        コレクションに新しいドキュメントを1つ挿入する。
        ドキュメントには自動的にowner_idが付与される。
        """
        with self._lock:
            # コレクションが存在しない場合は作成
            if collection_name not in self._db["collections"]:
                self._db["collections"][collection_name] = []

            doc_id = str(uuid.uuid4())  # UUIDを生成
            document["_id"] = doc_id  # ドキュメントに_idを追加

            # ドキュメントに所有者情報を追加
            document["owner_id"] = owner_id

            self._db["collections"][collection_name].append(document)

            self._update_indexes_on_insert(collection_name, document)
            
            return document

    def insert_many(self, collection_name: str, documents: list[dict], owner_id: str) -> list[dict]:
        """
        コレクションに複数のドキュメントを挿入する。
        各ドキュメントには自動的にowner_idが付与される。
        """
        with self._lock:
            if collection_name not in self._db["collections"]:
                self._db["collections"][collection_name] = []

            inserted_docs = []
            for document in documents:
                doc_id = str(uuid.uuid4())
                document["_id"] = doc_id
                document["owner_id"] = owner_id
                self._db["collections"][collection_name].append(document)
                self._update_indexes_on_insert(collection_name, document)
                inserted_docs.append(document)
            return inserted_docs

    def _update_indexes_on_insert(self, collection_name: str, document: dict):
        """新しいドキュメントが挿入されたときにインデックスを更新する内部メソッド"""
        if collection_name in self._indexes:
            for field, index_map in self._indexes[collection_name].items():
                if field in document:
                    index_map[document[field]] = document  # ドキュメント全体を保存

    def _update_indexes_on_update(
        self, collection_name: str, old_document: dict, new_document: dict
    ):
        """
        ドキュメントが更新されたときにインデックスを更新する内部メソッド。
        変更されたフィールドのみを更新する。
        """
        if collection_name in self._indexes:
            for field, index_map in self._indexes[collection_name].items():
                old_value = old_document.get(field)
                new_value = new_document.get(field)

                if old_value != new_value:
                    # 古いエントリを削除
                    if (
                        old_value in index_map
                        and index_map[old_value]["_id"] == old_document["_id"]
                    ):
                        del index_map[old_value]
                    # 新しいエントリを追加
                    if new_value is not None:
                        index_map[new_value] = new_document

    def _update_indexes_on_delete(self, collection_name: str, document: dict):
        """
        ドキュメントが削除されたときにインデックスを更新する内部メソッド。
        """
        if collection_name in self._indexes:
            for field, index_map in self._indexes[collection_name].items():
                if (
                    field in document
                    and document[field] in index_map
                    and index_map[document[field]]["_id"] == document["_id"]
                ):
                    del index_map[document[field]]

    def update_one(self, collection_name: str, query: dict, update_data: dict, owner_id: str) -> dict | None:
        """
        指定されたコレクションからクエリに一致するドキュメントを1つ更新する。
        owner_idに紐づくドキュメントのみを更新対象とする。
        _idとowner_idは更新できない。
        """
        with self._lock:
            if collection_name not in self._db["collections"]:
                return None
            
            for i, doc in enumerate(self._db["collections"][collection_name]):
                if doc.get("owner_id") == owner_id and query_engine.query_engine_instance.matches(doc, query):
                    # 更新前のドキュメントをコピー
                    old_doc = doc.copy()

                    # _idとowner_idは更新不可
                    if "_id" in update_data: del update_data["_id"]
                    if "owner_id" in update_data: del update_data["owner_id"]
                    
                    # ドキュメントを更新
                    doc.update(update_data)
                    self._db["collections"][collection_name][i] = doc
                    
                    # インデックスを効率的に更新
                    self._update_indexes_on_update(collection_name, old_doc, doc)
                    return doc
            return None

    def update_many(self, collection_name: str, query: dict, update_data: dict, owner_id: str) -> int:
        """
        指定されたコレクションからクエリに一致する複数のドキュメントを更新する。
        owner_idに紐づくドキュメントのみを更新対象とする。
        _idとowner_idは更新できない。
        更新されたドキュメントの数を返す。
        """
        with self._lock:
            if collection_name not in self._db["collections"]:
                return 0
            
            updated_count = 0
            for i, doc in enumerate(self._db["collections"][collection_name]):
                if doc.get("owner_id") == owner_id and query_engine.query_engine_instance.matches(doc, query):
                    old_doc = doc.copy()

                    if "_id" in update_data: del update_data["_id"]
                    if "owner_id" in update_data: del update_data["owner_id"]
                    
                    doc.update(update_data)
                    self._db["collections"][collection_name][i] = doc
                    
                    self._update_indexes_on_update(collection_name, old_doc, doc)
                    updated_count += 1
            return updated_count

    def delete_one(
        self, collection_name: str, query: dict, owner_id: str
    ) -> dict | None:
        """
        指定されたコレクションからクエリに一致するドキュメントを1つ削除する。
        owner_idに紐づくドキュメントのみを削除対象とする。
        """
        with self._lock:
            if collection_name not in self._db["collections"]:
                return None

            # 逆順にイテレートして削除してもインデックスがずれないようにする
            for i in range(len(self._db["collections"][collection_name]) - 1, -1, -1):
                doc = self._db["collections"][collection_name][i]
                if doc.get(
                    "owner_id"
                ) == owner_id and query_engine.query_engine_instance.matches(
                    doc, query
                ):
                    deleted_doc = self._db["collections"][collection_name].pop(i)
                    self._update_indexes_on_delete(
                        collection_name, deleted_doc
                    )  # インデックス更新
                    return deleted_doc
            return None

    def delete_many(self, collection_name: str, query: dict, owner_id: str) -> int:
        """
        指定されたコレクションからクエリに一致する複数のドキュメントを削除する。
        owner_idに紐づくドキュメントのみを削除対象とする。
        削除されたドキュメントの数を返す。
        """
        with self._lock:
            if collection_name not in self._db["collections"]:
                return 0

            deleted_count = 0
            # 逆順にイテレートして削除してもインデックスがずれないようにする
            for i in range(len(self._db["collections"][collection_name]) - 1, -1, -1):
                doc = self._db["collections"][collection_name][i]
                if doc.get("owner_id") == owner_id and query_engine.query_engine_instance.matches(doc, query):
                    deleted_doc = self._db["collections"][collection_name].pop(i)
                    self._update_indexes_on_delete(collection_name, deleted_doc)
                    deleted_count += 1
            return deleted_count

    def find_one(self, collection_name: str, query: dict, owner_id: str) -> dict | None:
        """
        指定されたコレクションからクエリに一致するドキュメントを1つ検索する。
        owner_idに紐づくドキュメントのみを返す。
        可能であればインデックスを利用する。
        """
        # find_manyを呼び出し、最初の結果を返す
        results = self.find_many(collection_name, query, owner_id)
        return results[0] if results else None

    def find_many(self, collection_name: str, query: dict, owner_id: str) -> list[dict]:
        """
        指定されたコレクションからクエリに一致するドキュメントを複数検索する。
        owner_idに紐づくドキュメントのみを返す。
        可能であればインデックスを利用する。
        """
        # findメソッドと実質的に同じなので、findを呼び出す
        return self.find(collection_name, query, owner_id)

    # --- インデックス管理API (スタブ) ---

    def create_index(self, collection_name: str, field: str):
        """指定されたフィールドのインデックスを作成する（将来の実装）"""
        with self._lock:
            logger.info(
                f"INFO: {collection_name}コレクションの{field}フィールドにインデックスを作成します。"
            )
            if collection_name not in self._indexes:
                self._indexes[collection_name] = {}
            self._indexes[collection_name][field] = {}

            # インデックス定義を永続化
            if collection_name not in self._db["_index_definitions"]:
                self._db["_index_definitions"][collection_name] = []
            if field not in self._db["_index_definitions"][collection_name]:
                self._db["_index_definitions"][collection_name].append(field)

            # 既存のドキュメントをループしてインデックスを構築
            if collection_name in self._db["collections"]:
                for doc in self._db["collections"][collection_name]:
                    if field in doc:
                        self._indexes[collection_name][field][doc[field]] = doc

    def _find_with_index(self, collection_name: str, query: dict) -> list[dict] | None:
        """クエリに最適なインデックスを探して検索を行う。見つからなければNoneを返す"""
        if collection_name not in self._indexes:
            return None

        # 最も選択率の高い（ユニークな値が多い）インデックスを探すロジック（今回は単純化）
        for field, value in query.items():
            if isinstance(value, dict): # $gt, $ltなどの演算子はインデックス検索の対象外（今回は単純化）
                continue
            if field in self._indexes[collection_name]:
                index_map = self._indexes[collection_name][field]
                if value in index_map:
                    # インデックスがヒットした場合、そのドキュメントをリストで返す
                    return [index_map[value]]
                else:
                    # インデックスはあるが、値が見つからない場合は空リストを返す
                    return []
        return None # 適切なインデックスが見つからなかった

    def find(self, collection_name: str, query: dict, owner_id: str) -> list[dict]:
        """
        指定されたコレクションからクエリに一致するドキュメントを検索する。
        owner_idに紐づくドキュメントのみを返す。
        可能であればインデックスを利用する。
        """
        with self._lock:
            if collection_name not in self._db["collections"]:
                return []

            # 1. インデックスを利用した検索を試みる
            indexed_results = self._find_with_index(collection_name, query)
            if indexed_results is not None:
                # インデックスで見つかった結果から、さらにowner_idとクエリでフィルタリング
                final_results = []
                for doc in indexed_results:
                    if doc.get("owner_id") == owner_id and query_engine.query_engine_instance.matches(doc, query):
                        final_results.append(doc)
                return final_results

            # 2. インデックスが利用できない場合はフルスキャン
            results = []
            for doc in self._db["collections"][collection_name]:
                # owner_idによるフィルタリング
                if doc.get("owner_id") == owner_id:
                    # クエリによるフィルタリング
                    if query_engine.query_engine_instance.matches(doc, query):
                        results.append(doc)
            return results


# --- シングルトンインスタンス ---
# アプリケーション全体で単一のデータベースインスタンスを共有する
db_instance = AstroDB()

if __name__ == "__main__":
    # --- モジュールの動作テスト ---
    logger.info("\n--- データベースコアのテスト実行 ---")

    # テスト用に既存のデータベースファイルを削除
    if DATABASE_FILE.exists():
        DATABASE_FILE.unlink()

    # 1. データベースインスタンスの初期化テスト
    logger.info("1. データベースの初期化")
    test_db = AstroDB()
    logger.info("データベースインスタンスは正常に作成されました。")

    # 2. ドキュメントの挿入テスト
    logger.info("\n2. ドキュメント挿入テスト")
    test_doc = {"name": "AstroDB", "type": "database"}
    # 本来このowner_idは認証エンジンから渡される
    inserted_doc = test_db.insert_one("projects", test_doc, owner_id="user_123")
    assert "owner_id" in inserted_doc
    assert inserted_doc["owner_id"] == "user_123"
    assert "_id" in inserted_doc  # _idが追加されたことを確認
    logger.info(
        f"ドキュメントが正常に挿入され、owner_idと_idが付与されました: {inserted_doc}"
    )

    # 3. インデックス作成テスト
    logger.info("\n3. インデックス作成テスト")
    test_db.create_index("projects", "name")
    assert "projects" in test_db._indexes
    assert "name" in test_db._indexes["projects"]
    assert test_db._indexes["projects"]["name"]["AstroDB"]["_id"] == inserted_doc["_id"]
    logger.info("インデックスが正常に作成され、既存データがインデックスされました。")

    # 4. データの永続化テスト
    logger.info("\n4. データベースの永続化テスト")
    test_db.save_to_disk()
    assert DATABASE_FILE.exists()
    logger.info(f"{DATABASE_FILE} が正常に作成/更新されました。")

    # 5. データベースの読み込みとインデックス再構築テスト
    logger.info("\n5. データベースの読み込みとインデックス再構築テスト")
    new_db_instance = AstroDB()
    assert "projects" in new_db_instance._db["collections"]
    retrieved_doc = new_db_instance._db["collections"]["projects"][0]
    assert retrieved_doc["name"] == "AstroDB"
    assert "_id" in retrieved_doc
    # インデックスが再構築されたことを確認
    assert "projects" in new_db_instance._indexes
    assert "name" in new_db_instance._indexes["projects"]
    assert (
        new_db_instance._indexes["projects"]["name"]["AstroDB"]["_id"]
        == retrieved_doc["_id"]
    )
    logger.info(
        f"ファイルからデータベースを正常に再読み込みし、インデックスも再構築されました。"
    )

    # 6. 新しいドキュメント挿入時のインデックス更新テスト
    logger.info("\n6. 新しいドキュメント挿入時のインデックス更新テスト")
    test_doc2 = {"name": "NewProject", "type": "app"}
    inserted_doc2 = new_db_instance.insert_one(
        "projects", test_doc2, owner_id="user_456"
    )
    assert "name" in new_db_instance._indexes["projects"]
    assert (
        new_db_instance._indexes["projects"]["name"]["NewProject"]["_id"]
        == inserted_doc2["_id"]
    )
    logger.info("新しいドキュメント挿入時にインデックスが正常に更新されました。")

    logger.info("\nテスト成功！データベースコアが正常に機能しています。")
    # テスト用に作成されたファイルをクリーンアップ
    DATABASE_FILE.unlink()
    logger.info(f"テストファイル {DATABASE_FILE} を削除しました。")
