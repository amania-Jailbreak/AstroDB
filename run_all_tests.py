import os
from pathlib import Path
import sys
import logging
from dotenv import load_dotenv, set_key

# ロガーの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# プロジェクトのルートディレクトリをPythonのパスに追加
sys.path.append(str(Path(__file__).parent))

# テスト対象のモジュールをインポート
import auth_engine
import query_engine


# --- テストユーティリティ関数 ---
def run_test(test_func, description):
    logger.info(f"\n--- {description} ---")
    try:
        test_func()
        logger.info(f"{description}: 成功")
    except AssertionError as e:
        logger.error(f"{description}: 失敗 - {e}")
    except Exception as e:
        logger.error(f"{description}: エラー - {e}")


def setup_auth_engine_test():
    # .envファイルをクリーンアップ
    env_path = auth_engine.ENV_FILE_PATH
    if os.path.exists(env_path):
        os.remove(env_path)
    # _users_db をリセット
    auth_engine._users_db = {}


def teardown_auth_engine_test():
    # .envファイルをクリーンアップ
    env_path = auth_engine.ENV_FILE_PATH
    if os.path.exists(env_path):
        os.remove(env_path)
    # _users_db をリセット
    auth_engine._users_db = {}


# --- auth_engine.py のテスト関数 ---
def test_auth_engine_get_or_generate_jwt_secret_key():
    # 初回呼び出しでキーが生成されることを確認
    key1 = auth_engine.get_or_generate_jwt_secret_key()
    assert key1 is not None, "キーがNoneです"
    assert isinstance(key1, str), "キーが文字列ではありません"
    assert len(key1) > 0, "キーが空です"

    # 2回目以降の呼び出しで既存のキーが読み込まれることを確認
    key2 = auth_engine.get_or_generate_jwt_secret_key()
    assert key1 == key2, "2回目の呼び出しで異なるキーが生成されました"

    # .envファイルにキーが保存されていることを確認
    load_dotenv(override=True)  # .envファイルを再読み込み
    assert os.getenv("JWT_SECRET_KEY") == key1, ".envファイルにキーが保存されていません"


def test_auth_engine_hash_and_verify_password():
    password = "mysecretpassword"
    hashed_password = auth_engine.hash_password(password)
    assert isinstance(
        hashed_password, bytes
    ), "ハッシュ化されたパスワードがbytesではありません"
    assert (
        auth_engine.verify_password(password, hashed_password) is True
    ), "パスワードの検証に失敗しました"
    assert (
        auth_engine.verify_password("wrongpassword", hashed_password) is False
    ), "誤ったパスワードの検証が成功しました"


def test_auth_engine_create_and_decode_access_token():
    user_data = {"sub": "testuser", "role": "user"}
    token = auth_engine.create_access_token(user_data)
    assert isinstance(token, str), "トークンが文字列ではありません"

    decoded_payload = auth_engine.decode_access_token(token)
    assert decoded_payload is not None, "トークンのデコードに失敗しました"
    assert (
        decoded_payload["sub"] == "testuser"
    ), "デコードされたペイロードのsubが一致しません"
    assert (
        decoded_payload["role"] == "user"
    ), "デコードされたペイロードのroleが一致しません"

    # 無効なトークンのテスト
    invalid_token = token[:-5] + "invalid"
    assert (
        auth_engine.decode_access_token(invalid_token) is None
    ), "無効なトークンのデコードが成功しました"


def test_auth_engine_register_user():
    # ユーザー登録
    assert (
        auth_engine.register_user("testuser_reg", "password123") is True
    ), "ユーザー登録に失敗しました"
    # 既存ユーザーの登録
    assert (
        auth_engine.register_user("testuser_reg", "another_password") is False
    ), "既存ユーザーの登録が成功しました"
    # 管理者ユーザーの登録
    assert (
        auth_engine.register_user("adminuser_reg", "adminpass", role="admin") is True
    ), "管理者ユーザーの登録に失敗しました"


def test_auth_engine_authenticate_user():
    auth_engine.register_user("testuser_auth", "password123")
    # 認証成功
    authenticated_user = auth_engine.authenticate_user("testuser_auth", "password123")
    assert authenticated_user is not None, "認証に失敗しました"
    assert (
        authenticated_user["username"] == "testuser_auth"
    ), "認証されたユーザー名が一致しません"
    assert (
        authenticated_user["role"] == "user"
    ), "認証されたユーザーのロールが一致しません"
    # パスワード間違い
    assert (
        auth_engine.authenticate_user("testuser_auth", "wrongpassword") is None
    ), "誤ったパスワードで認証が成功しました"
    # 存在しないユーザー
    assert (
        auth_engine.authenticate_user("nonexistentuser_auth", "password123") is None
    ), "存在しないユーザーで認証が成功しました"


def test_auth_engine_change_password():
    auth_engine.register_user("testuser_cp", "oldpass")
    # パスワード変更成功
    assert (
        auth_engine.change_password("testuser_cp", "oldpass", "newpass") is True
    ), "パスワード変更に失敗しました"
    # 新しいパスワードで認証
    assert (
        auth_engine.authenticate_user("testuser_cp", "newpass") is not None
    ), "新しいパスワードで認証できませんでした"
    # 古いパスワードで認証失敗
    assert (
        auth_engine.authenticate_user("testuser_cp", "oldpass") is None
    ), "古いパスワードで認証が成功しました"
    # 存在しないユーザーのパスワード変更
    assert (
        auth_engine.change_password("nonexistent_cp", "oldpass", "newpass") is False
    ), "存在しないユーザーのパスワード変更が成功しました"
    # 間違った古いパスワード
    assert (
        auth_engine.change_password("testuser_cp", "wrongoldpass", "newpass2") is False
    ), "誤った古いパスワードでパスワード変更が成功しました"


# --- 全てのテストを実行するメイン関数 ---
def test_query_engine_simple_match():
    engine = query_engine.QueryEngine()
    doc1 = {"title": "AstroDB", "author": "Amani", "year": 2025}
    doc2 = {"title": "WebApp", "author": "Amani", "year": 2024}
    query1 = {"author": "Amani"}
    assert engine.matches(doc1, query1) is True, "シンプルな一致テスト (doc1) が失敗しました"
    assert engine.matches(doc2, query1) is True, "シンプルな一致テスト (doc2) が失敗しました"

def test_query_engine_multiple_conditions():
    engine = query_engine.QueryEngine()
    doc1 = {"title": "AstroDB", "author": "Amani", "year": 2025}
    doc2 = {"title": "WebApp", "author": "Amani", "year": 2024}
    query2 = {"author": "Amani", "year": 2025}
    assert engine.matches(doc1, query2) is True, "複数条件での一致テスト (doc1) が失敗しました"
    assert engine.matches(doc2, query2) is False, "複数条件での一致テスト (doc2) が失敗しました"

def test_query_engine_non_existent_key():
    engine = query_engine.QueryEngine()
    doc1 = {"title": "AstroDB", "author": "Amani", "year": 2025}
    query3 = {"status": "published"}
    assert engine.matches(doc1, query3) is False, "存在しないキーでのテストが失敗しました"

def test_query_engine_array_exact_match():
    engine = query_engine.QueryEngine()
    doc1 = {"title": "AstroDB", "tags": ["db", "python"]}
    query4 = {"tags": ["db", "python"]}
    assert engine.matches(doc1, query4) is True, "配列の完全一致テストが失敗しました"

def test_query_engine_gt_operator():
    engine = query_engine.QueryEngine()
    doc1 = {"year": 2025}
    query_gt = {"year": {"$gt": 2024}}
    assert engine.matches(doc1, query_gt) is True, "$gt 演算子のテストが失敗しました"

def test_query_engine_lt_operator():
    engine = query_engine.QueryEngine()
    doc1 = {"year": 2025}
    query_lt = {"year": {"$lt": 2025}}
    assert engine.matches(doc1, query_lt) is False, "$lt 演算子のテストが失敗しました"

def test_query_engine_gte_operator():
    engine = query_engine.QueryEngine()
    doc1 = {"year": 2025}
    query_gte = {"year": {"$gte": 2025}}
    assert engine.matches(doc1, query_gte) is True, "$gte 演算子のテストが失敗しました"

def test_query_engine_lte_operator():
    engine = query_engine.QueryEngine()
    doc1 = {"year": 2025}
    query_lte = {"year": {"$lte": 2024}}
    assert engine.matches(doc1, query_lte) is False, "$lte 演算子のテストが失敗しました"

def test_query_engine_ne_operator():
    engine = query_engine.QueryEngine()
    doc1 = {"author": "Amani"}
    query_ne = {"author": {"$ne": "Amani"}}
    assert engine.matches(doc1, query_ne) is False, "$ne 演算子のテストが失敗しました"

def test_query_engine_in_operator():
    engine = query_engine.QueryEngine()
    doc1 = {"tags": ["db", "python"]}
    doc2 = {"tags": ["web", "js"]}
    query_in = {"tags": {"$in": ["js", "sql"]}}
    assert engine.matches(doc1, query_in) is False, "$in 演算子のテスト (doc1) が失敗しました"
    assert engine.matches(doc2, query_in) is True, "$in 演算子のテスト (doc2) が失敗しました"

def test_query_engine_nin_operator():
    engine = query_engine.QueryEngine()
    doc1 = {"tags": ["db", "python"]}
    doc2 = {"tags": ["web", "js"]}
    query_nin = {"tags": {"$nin": ["js", "sql"]}}
    assert engine.matches(doc1, query_nin) is True, "$nin 演算子のテスト (doc1) が失敗しました"
    assert engine.matches(doc2, query_nin) is False, "$nin 演算子のテスト (doc2) が失敗しました"

def test_query_engine_and_operator():
    engine = query_engine.QueryEngine()
    doc = {"name": "test", "age": 30, "city": "Tokyo"}
    query = {"$and": [{"age": {"$gt": 25}}, {"city": "Tokyo"}]}
    assert engine.matches(doc, query) is True, "$and 演算子のテストが失敗しました"

    query_false = {"$and": [{"age": {"$gt": 35}}, {"city": "Tokyo"}]}
    assert engine.matches(doc, query_false) is False, "$and 演算子の失敗テストが失敗しました"

def test_query_engine_or_operator():
    engine = query_engine.QueryEngine()
    doc = {"name": "test", "age": 30, "city": "Tokyo"}
    query = {"$or": [{"age": {"$lt": 25}}, {"city": "Tokyo"}]}
    assert engine.matches(doc, query) is True, "$or 演算子のテストが失敗しました"

    query_false = {"$or": [{"age": {"$lt": 25}}, {"city": "Osaka"}]}
    assert engine.matches(doc, query_false) is False, "$or 演算子の失敗テストが失敗しました"

def test_query_engine_nested_and_or():
    engine = query_engine.QueryEngine()
    doc = {"name": "test", "age": 30, "city": "Tokyo", "status": "active"}
    query = {"$and": [
        {"$or": [{"age": {"$lt": 25}}, {"city": "Tokyo"}]},
        {"status": "active"}
    ]}
    assert engine.matches(doc, query) is True, "ネストされた $and/$or 演算子のテストが失敗しました"

    query_false = {"$and": [
        {"$or": [{"age": {"$lt": 25}}, {"city": "Osaka"}]},
        {"status": "active"}
    ]}
    assert engine.matches(doc, query_false) is False, "ネストされた $and/$or 演算子の失敗テストが失敗しました"

def test_query_engine_nested_and_or():
    engine = query_engine.QueryEngine()
    doc = {"name": "test", "age": 30, "city": "Tokyo", "status": "active"}
    query = {"$and": [
        {"$or": [{"age": {"$lt": 25}}, {"city": "Tokyo"}]},
        {"status": "active"}
    ]}
    assert engine.matches(doc, query) is True, "ネストされた $and/$or 演算子のテストが失敗しました"

    query_false = {"$and": [
        {"$or": [{"age": {"$lt": 25}}, {"city": "Osaka"}]},
        {"status": "active"}
    ]}
    assert engine.matches(doc, query_false) is False, "ネストされた $and/$or 演算子の失敗テストが失敗しました"

def test_query_engine_regex_operator():
    engine = query_engine.QueryEngine()
    doc1 = {"title": "AstroDB", "author": "Amani", "year": 2025, "tags": ["db", "python"], "user": {"profile": {"age": 30, "city": "Tokyo"}}}
    doc2 = {"title": "WebApp", "author": "Amani", "year": 2024, "tags": ["web", "js"], "user": {"profile": {"age": 25, "city": "Osaka"}}}
    doc3 = {"title": "AstroDB Guide", "author": "Gemini", "year": 2025, "tags": ["db", "guide"], "user": {"profile": {"age": 30, "city": "Tokyo"}}}
    query_regex = {"title": {"$regex": "^Astro"}}
    assert engine.matches(doc1, query_regex) is True, "$regex 演算子のテスト (doc1) が失敗しました"
    assert engine.matches(doc3, query_regex) is True, "$regex 演算子のテスト (doc3) が失敗しました"
    assert engine.matches(doc2, query_regex) is False, "$regex 演算子のテスト (doc2) が失敗しました"

def test_query_engine_size_operator():
    engine = query_engine.QueryEngine()
    doc1 = {"title": "AstroDB", "tags": ["db", "python"]}
    doc6 = {"title": "Mixed Tags", "tags": ["python", "ai", "ml"]}
    doc7 = {"title": "Single Tag", "tags": ["python"]}
    query_size = {"tags": {"$size": 2}}
    assert engine.matches(doc1, query_size) is True, "$size 演算子のテスト (doc1) が失敗しました"
    assert engine.matches(doc6, query_size) is False, "$size 演算子のテスト (doc6) が失敗しました"
    assert engine.matches(doc7, query_size) is False, "$size 演算子のテスト (doc7) が失敗しました"

def test_query_engine_exists_operator():
    engine = query_engine.QueryEngine()
    doc1 = {"title": "AstroDB", "author": "Amani"}
    doc5 = {"title": "Array Test", "items": [{"id": 1, "value": "A"}, {"id": 2, "value": "B"}]}
    query_exists_true = {"author": {"$exists": True}}
    assert engine.matches(doc1, query_exists_true) is True, "$exists 演算子のテスト (True) が失敗しました"
    assert engine.matches(doc5, query_exists_true) is False, "$exists 演算子のテスト (False) が失敗しました"
    query_exists_false = {"non_existent_field": {"$exists": False}}
    assert engine.matches(doc1, query_exists_false) is True, "$exists 演算子のテスト (非存在) が失敗しました"

def main():    logger.info("=====================================")    logger.info("  AstroDB 全テスト実行 (Pytestなし)")    logger.info("=====================================")    # auth_engine.py のテスト    # test_auth_engine_get_or_generate_jwt_secret_key は特殊なセットアップが必要    logger.info("\n--- auth_engine: JWT秘密鍵の生成と読み込み ---")    try:        env_path = auth_engine.ENV_FILE_PATH        if os.path.exists(env_path):            os.remove(env_path)        # 環境変数をクリア        if "JWT_SECRET_KEY" in os.environ:            del os.environ["JWT_SECRET_KEY"]        test_auth_engine_get_or_generate_jwt_secret_key()        logger.info("auth_engine: JWT秘密鍵の生成と読み込み: 成功")    except AssertionError as e:        logger.error(f"auth_engine: JWT秘密鍵の生成と読み込み: 失敗 - {e}")    except Exception as e:        logger.error(f"auth_engine: JWT秘密鍵の生成と読み込み: エラー - {e}")    finally:        if os.path.exists(env_path):            os.remove(env_path)

    setup_auth_engine_test()
    run_test(
        test_auth_engine_hash_and_verify_password,
        "auth_engine: パスワードのハッシュ化と検証",
    )
    teardown_auth_engine_test()

    setup_auth_engine_test()
    run_test(
        test_auth_engine_create_and_decode_access_token,
        "auth_engine: JWTの生成とデコード",
    )
    teardown_auth_engine_test()

    setup_auth_engine_test()
    run_test(test_auth_engine_register_user, "auth_engine: ユーザー登録")
    teardown_auth_engine_test()

    setup_auth_engine_test()
    run_test(test_auth_engine_authenticate_user, "auth_engine: ユーザー認証")
    teardown_auth_engine_test()

    setup_auth_engine_test()
    run_test(test_auth_engine_change_password, "auth_engine: パスワード変更")
    teardown_auth_engine_test()

    # query_engine.py のテスト
    logger.info("\n--- query_engine.py のテスト ---")
    run_test(test_query_engine_simple_match, "query_engine: シンプルな一致テスト")
    run_test(test_query_engine_multiple_conditions, "query_engine: 複数条件での一致テスト")
    run_test(test_query_engine_non_existent_key, "query_engine: 存在しないキーでのテスト")
    run_test(test_query_engine_array_exact_match, "query_engine: 配列の完全一致テスト")
    run_test(test_query_engine_gt_operator, "query_engine: $gt 演算子のテスト")
    run_test(test_query_engine_lt_operator, "query_engine: $lt 演算子のテスト")
    run_test(test_query_engine_gte_operator, "query_engine: $gte 演算子のテスト")
    run_test(test_query_engine_lte_operator, "query_engine: $lte 演算子のテスト")
    run_test(test_query_engine_ne_operator, "query_engine: $ne 演算子のテスト")
    run_test(test_query_engine_in_operator, "query_engine: $in 演算子のテスト")
    run_test(test_query_engine_nin_operator, "query_engine: $nin 演算子のテスト")
    run_test(test_query_engine_and_operator, "query_engine: $and 演算子のテスト")
    run_test(test_query_engine_or_operator, "query_engine: $or 演算子のテスト")
    run_test(test_query_engine_nested_and_or, "query_engine: ネストされた $and/$or 演算子のテスト")
    run_test(test_query_engine_regex_operator, "query_engine: $regex 演算子のテスト")
    run_test(test_query_engine_size_operator, "query_engine: $size 演算子のテスト")
    run_test(test_query_engine_exists_operator, "query_engine: $exists 演算子のテスト")

    logger.info("\n=====================================")
    logger.info("  全てのテストが完了しました。")
    logger.info("=====================================")


if __name__ == "__main__":
    main()

