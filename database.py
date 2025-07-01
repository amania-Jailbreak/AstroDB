
import ujson
from threading import Lock
from pathlib import Path

# プロジェクトのモジュールをインポート
import encryption

# --- 定数 --- 
DATABASE_FILE = Path("database.json.encrypted")

class AstroDB:
    def __init__(self):
        self._db = {}  # データストア本体例: {"collections": {"users": [...], "posts": [...]}}
        self._indexes = {} # インデックスデータ例: {"users": {"email": {"a@b.com": doc_id}}}
        self._lock = Lock() # スレッドセーフのためのロック
        self.load_from_disk()

    def load_from_disk(self):
        """暗号化されたデータベースファイルをディスクから読み込む"""
        with self._lock:
            if not DATABASE_FILE.exists():
                print("データベースファイルが存在しないため、新しいデータベースを作成します。")
                self._db = {"collections": {}}
                return

            try:
                with open(DATABASE_FILE, "rb") as f:
                    encrypted_data = f.read()
                
                if not encrypted_data:
                    print("データベースファイルが空です。新しいデータベースを作成します。")
                    self._db = {"collections": {}}
                    return

                decrypted_json = encryption.decrypt(encrypted_data)
                self._db = ujson.loads(decrypted_json)
                print("データベースをディスクから正常に読み込みました。")
                # TODO: データベースからインデックスを再構築する

            except Exception as e:
                print(f"致命的エラー: データベースの読み込みに失敗しました。 {e}")
                # ファイルが破損している可能性などを考慮し、空のDBで起動
                self._db = {"collections": {}}

    def save_to_disk(self):
        """メモリ上のデータベースを暗号化してディスクに保存する"""
        with self._lock:
            try:
                db_json = ujson.dumps(self._db)
                encrypted_data = encryption.encrypt(db_json)
                with open(DATABASE_FILE, "wb") as f:
                    f.write(encrypted_data)
                print("データベースをディスクに正常に保存しました。")
            except Exception as e:
                print(f"エラー: データベースの保存に失敗しました。 {e}")

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
            
            # ドキュメントに所有者情報を追加
            document["owner_id"] = owner_id
            # TODO: 一意のID (_id) を付与する

            self._db["collections"][collection_name].append(document)
            
            # TODO: 新しいドキュメントに対してインデックスを更新する
            
            return document

    # --- インデックス管理API (スタブ) --- 

    def create_index(self, collection_name: str, field: str):
        """指定されたフィールドのインデックスを作成する（将来の実装）"""
        with self._lock:
            print(f"INFO: {collection_name}コレクションの{field}フィールドにインデックスを作成します。（現在未実装）")
            if collection_name not in self._indexes:
                self._indexes[collection_name] = {}
            self._indexes[collection_name][field] = {}
            # TODO: 既存のデータからインデックスを構築する処理
            pass

# --- シングルトンインスタンス --- 
# アプリケーション全体で単一のデータベースインスタンスを共有する
db_instance = AstroDB()

if __name__ == '__main__':
    # --- モジュールの動作テスト ---
    print("\n--- データベースコアのテスト実行 ---")

    # 1. データベースインスタンスの初期化テスト
    # このスクリプトを実行すると、AstroDB()がインスタンス化され、
    # "database.json.encrypted" がなければ作成される。
    print("1. データベースの初期化")
    test_db = AstroDB()
    print("データベースインスタンスは正常に作成されました。")

    # 2. ドキュメントの挿入テスト
    print("\n2. ドキュメント挿入テスト")
    test_doc = {"name": "AstroDB", "type": "database"}
    # 本来このowner_idは認証エンジンから渡される
    inserted_doc = test_db.insert_one("projects", test_doc, owner_id="user_123")
    assert "owner_id" in inserted_doc
    assert inserted_doc["owner_id"] == "user_123"
    print(f"ドキュメントが正常に挿入され、owner_idが付与されました: {inserted_doc}")

    # 3. データの永続化テスト
    print("\n3. データベースの永続化テスト")
    test_db.save_to_disk()
    assert DATABASE_FILE.exists()
    print(f"{DATABASE_FILE} が正常に作成/更新されました。")

    # 4. データの読み込みテスト
    print("\n4. データベースの読み込みテスト")
    new_db_instance = AstroDB()
    assert "projects" in new_db_instance._db["collections"]
    retrieved_doc = new_db_instance._db["collections"]["projects"][0]
    assert retrieved_doc["name"] == "AstroDB"
    print(f"ファイルからデータベースを正常に再読み込みできました。")

    print("\nテスト成功！データベースコアが正常に機能しています。")
    # テスト用に作成されたファイルをクリーンアップ
    DATABASE_FILE.unlink()
    print(f"テストファイル {DATABASE_FILE} を削除しました。")
