
import os
from dotenv import load_dotenv, set_key
from cryptography.fernet import Fernet, InvalidToken
import logging

# ロガーの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# .envファイルから環境変数を読み込む
# .envファイルが存在しない場合は、この時点では何もしない
load_dotenv()

ENV_FILE_PATH = ".env"

def get_or_generate_key() -> bytes:
    """
    .envファイルから暗号化キーを読み込む。
    キーが存在しない場合は、新しいキーを生成して.envファイルに保存する。
    """
    key = os.getenv("ENCRYPTION_KEY")
    if key:
        return key.encode()

    # 新しいキーを生成
    new_key = Fernet.generate_key()
    
    # .envファイルにキーを保存
    # ファイルが存在しない場合は新規作成される
    set_key(ENV_FILE_PATH, "ENCRYPTION_KEY", new_key.decode())
    
    logger.info("新しい暗号化キーを生成し、.envファイルに保存しました。")
    return new_key

# モジュール読み込み時にキーを準備
try:
    encryption_key = get_or_generate_key()
    fernet = Fernet(encryption_key)
except Exception as e:
    logger.critical(f"致命的なエラー: 暗号化キーの初期化に失敗しました。{e}")
    # 実際のアプリケーションでは、ここで終了するか、エラー処理を行う
    fernet = None

def encrypt(data: str) -> bytes:
    """
    文字列データを暗号化する。
    """
    if not fernet:
        raise ConnectionAbortedError("暗号化モジュールが初期化されていません。")
    return fernet.encrypt(data.encode('utf-8'))

def decrypt(encrypted_data: bytes) -> str:
    """
    暗号化されたデータを復号する。
    """
    if not fernet:
        raise ConnectionAbortedError("暗号化モジュールが初期化されていません。")
    try:
        decrypted_data = fernet.decrypt(encrypted_data)
        return decrypted_data.decode('utf-8')
    except InvalidToken:
        # キーが違う、データが破損しているなどの場合に発生
        raise ValueError("復号に失敗しました。データが破損しているか、キーが不正です。")

if __name__ == '__main__':
    # モジュールの動作テスト
    logger.info("--- 暗号化モジュールのテスト実行 ---")
    
    # .envファイルにキーがなければ生成される
    logger.info(f"使用されるキー (最初の10文字): {encryption_key.decode()[:10]}...")

    original_text = '{"command": "SET", "key": "mykey", "value": "これは秘密の情報です"}'
    logger.info(f"元のデータ: {original_text}")

    # 暗号化
    encrypted = encrypt(original_text)
    logger.info(f"暗号化されたデータ (一部): {encrypted[:50]}...")

    # 復号
    decrypted = decrypt(encrypted)
    logger.info(f"復号されたデータ: {decrypted}")

    # テスト検証
    assert original_text == decrypted
    logger.info("\nテスト成功！暗号化と復号が正常に機能しています。")
