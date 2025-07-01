import bcrypt
import jwt
from datetime import datetime, timedelta, timezone
import os
import logging
from dotenv import load_dotenv, set_key

# ロガーの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# .envファイルから環境変数を読み込む
load_dotenv(override=True)

ENV_FILE_PATH = ".env"


def get_or_generate_jwt_secret_key() -> str:
    """
    .envファイルからJWTの秘密鍵を読み込む。
    キーが存在しない場合は、新しいキーを生成して.envファイルに保存する。
    """
    key = os.getenv("JWT_SECRET_KEY")
    if key:
        return key

    # 新しいキーを生成 (ランダムな文字列)
    new_key = os.urandom(32).hex()

    # .envファイルにキーを保存
    set_key(ENV_FILE_PATH, "JWT_SECRET_KEY", new_key)

    # 保存されているか確認
    saved_key = os.getenv("JWT_SECRET_KEY")
    if saved_key != new_key:
        # .envファイルに書き込んだ後、再度読み込む必要がある場合がある
        load_dotenv(override=True)
        saved_key = os.getenv("JWT_SECRET_KEY")
        if saved_key != new_key:
            logger.error("JWT秘密鍵の保存に失敗しました。")
    else:
        logger.info("新しいJWT秘密鍵を生成し、.envファイルに保存しました。")
    return new_key


# --- 設定 ---
SECRET_KEY = get_or_generate_jwt_secret_key()
ALGORITHM = "HS256"  # 署名アルゴリズム
ACCESS_TOKEN_EXPIRE_MINUTES = 30  # トークンの有効期間

# --- 認証レートリミット設定 ---
MAX_LOGIN_ATTEMPTS = 5  # ロックアウトされるまでの最大試行回数
LOCKOUT_TIME_MINUTES = 5  # ロックアウトされる時間（分）

# 失敗したログイン試行を追跡するための辞書
# key: username, value: {'count': int, 'last_attempt': datetime}
_failed_login_attempts = {}

# --- データベースのダミー ---
# 本来はdatabase.pyと連携するが、ここではメモリ上の辞書で代用
# key: username, value: {hashed_password: str, role: str}
_users_db = {}


def hash_password(password: str) -> bytes:
    """パスワードをハッシュ化する"""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())


def verify_password(plain_password: str, hashed_password: bytes) -> bool:
    """平文パスワードがハッシュ化されたパスワードと一致するか検証する"""
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password)


def create_access_token(data: dict) -> str:
    """指定されたデータを含むJWTを生成する"""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def decode_access_token(token: str) -> dict | None:
    """JWTをデコードしてペイロード（中身）を返す。無効な場合はNoneを返す"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.PyJWTError:
        return None


# --- ユーザー管理API ---


def register_user(username: str, password: str, role: str = "user") -> bool:
    """ユーザーを登録する。成功したらTrue、ユーザーが既に存在する場合はFalseを返す"""
    if username in _users_db:
        return False

    hashed_pw = hash_password(password)
    _users_db[username] = {"hashed_password": hashed_pw, "role": role}
    return True


def authenticate_user(username: str, password: str) -> dict | None:
    """
    ユーザーを認証する。
    成功した場合はユーザー情報（ロールなど）を、失敗した場合はNoneを返す。
    """
    # ロックアウト状態のチェック
    if username in _failed_login_attempts:
        last_attempt_time = _failed_login_attempts[username]['last_attempt']
        if datetime.now(timezone.utc) - last_attempt_time < timedelta(minutes=LOCKOUT_TIME_MINUTES):
            logger.warning(f"ユーザー {username} はロックアウト中です。")
            return None

    user = _users_db.get(username)
    if not user:
        logger.warning(f"認証失敗: ユーザー {username} が見つかりません。")
        return None

    if not verify_password(password, user["hashed_password"]):
        # ログイン失敗を記録
        _failed_login_attempts.setdefault(username, {'count': 0, 'last_attempt': datetime.now(timezone.utc)})
        _failed_login_attempts[username]['count'] += 1
        _failed_login_attempts[username]['last_attempt'] = datetime.now(timezone.utc)
        logger.warning(f"認証失敗: ユーザー {username} のパスワードが不正です。試行回数: {_failed_login_attempts[username]['count']}")

        if _failed_login_attempts[username]['count'] >= MAX_LOGIN_ATTEMPTS:
            logger.warning(f"ユーザー {username} がロックアウトされました。{LOCKOUT_TIME_MINUTES}分間ログインできません。")

        return None

    # 認証成功時は失敗回数をリセット
    if username in _failed_login_attempts:
        del _failed_login_attempts[username]
        logger.info(f"ユーザー {username} のログイン成功。失敗回数をリセットしました。")

    return {"username": username, "role": user["role"]}


def change_password(username: str, old_password: str, new_password: str) -> bool:
    """
    ユーザーのパスワードを変更する。
    古いパスワードが正しく、ユーザーが存在すればTrueを返す。
    """
    user = _users_db.get(username)
    if not user:
        return False

    if not verify_password(old_password, user["hashed_password"]):
        return False

    hashed_pw = hash_password(new_password)
    _users_db[username]["hashed_password"] = hashed_pw
    return True


if __name__ == "__main__":
    # --- モジュールの動作テスト ---
    logger.info("--- 認証・認可エンジンのテスト実行 ---")

    # 1. ユーザー登録
    logger.info("\n1. ユーザー登録テスト")
    assert register_user("testuser", "password123") is True
    logger.info("ユーザー 'testuser' の登録成功。")
    assert register_user("testuser", "another_password") is False
    logger.info("既存ユーザーの登録失敗を正常に検知。")
    register_user("adminuser", "adminpass", role="admin")
    logger.info("管理者 'adminuser' の登録成功。")

    # 2. ユーザー認証
    logger.info("\n2. ユーザー認証テスト")
    authenticated_user = authenticate_user("testuser", "password123")
    assert authenticated_user is not None
    assert authenticated_user["role"] == "user"
    logger.info("'testuser' の認証成功。")
    assert authenticate_user("testuser", "wrongpassword") is None
    logger.info("パスワード間違い時の認証失敗を正常に検知。")
    assert authenticate_user("nonexistentuser", "password123") is None
    logger.info("存在しないユーザーの認証失敗を正常に検知。")

    # 3. JWTの発行と検証
    logger.info("\n3. JWT発行・検証テスト")
    user_data = {
        "sub": authenticated_user["username"],
        "role": authenticated_user["role"],
    }
    token = create_access_token(user_data)
    logger.info(f"発行されたトークン (一部): {token[:30]}...")
    assert isinstance(token, str)

    decoded_payload = decode_access_token(token)
    assert decoded_payload is not None
    assert decoded_payload["sub"] == "testuser"
    assert decoded_payload["role"] == "user"
    logger.info("トークンのデコードと内容の検証に成功。")

    invalid_token = token[:-5] + "invalid"
    assert decode_access_token(invalid_token) is None
    logger.info("無効なトークンの検証失敗を正常に検知。")

    logger.info("\nテスト成功！認証・認可エンジンが正常に機能しています。")
