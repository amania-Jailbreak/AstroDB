import shutil
from pathlib import Path
from datetime import datetime
import logging

# ロガーの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# プロジェクトのモジュールをインポート
import database

DATABASE_FILE = database.DATABASE_FILE

BACKUP_DIR = Path("backups")

def backup_database() -> str:
    """
    現在のデータベースファイルをバックアップディレクトリにコピーする。
    バックアップファイル名にはタイムスタンプが付与される。
    """
    if not DATABASE_FILE.exists():
        return "エラー: データベースファイルが存在しません。"

    BACKUP_DIR.mkdir(exist_ok=True) # バックアップディレクトリが存在しない場合は作成

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = BACKUP_DIR / f"database_{timestamp}.adb.bak"

    try:
        shutil.copy(DATABASE_FILE, backup_file)
        return f"データベースを {backup_file} にバックアップしました。"
    except Exception as e:
        return f"エラー: データベースのバックアップに失敗しました。 {e}"

def restore_database(backup_filename: str) -> str:
    """
    指定されたバックアップファイルからデータベースを復元する。
    """
    backup_file_path = BACKUP_DIR / backup_filename

    if not backup_file_path.exists():
        return f"エラー: バックアップファイル '{backup_filename}' が見つかりません。"

    try:
        shutil.copy(backup_file_path, DATABASE_FILE)
        # データベースインスタンスを再ロードして変更を反映
        from database import db_instance
        db_instance.load_from_disk()
        return f"データベースを '{backup_filename}' から復元しました。"
    except Exception as e:
        return f"エラー: データベースの復元に失敗しました。 {e}"


if __name__ == '__main__':
    # --- モジュールの動作テスト ---
    logger.info("\n--- 自動化エンジンのテスト実行 ---")

    # テスト用にダミーのデータベースファイルを作成
    dummy_db_content = b"dummy_encrypted_data"
    DATABASE_FILE.write_bytes(dummy_db_content)
    logger.info(f"ダミーデータベースファイル {DATABASE_FILE} を作成しました。")

    # 1. バックアップテスト
    logger.info("\n1. バックアップテスト")
    backup_result = backup_database()
    logger.info(backup_result)
    assert "バックアップしました" in backup_result

    # 2. 復元テスト (最新のバックアップファイルを取得)
    logger.info("\n2. 復元テスト")
    # 最新のバックアップファイルを見つける
    latest_backup = None
    latest_timestamp = datetime.min
    for f in BACKUP_DIR.iterdir():
        if f.is_file() and f.name.startswith("database_") and f.name.endswith(".adb.bak"):
            try:
                # ファイル名からタイムスタンプを抽出
                ts_str = f.name.split("_")[1].split(".")[0]
                file_timestamp = datetime.strptime(ts_str, "%Y%m%d_%H%M%S")
                if file_timestamp > latest_timestamp:
                    latest_timestamp = file_timestamp
                    latest_backup = f.name
            except ValueError:
                continue # 無効なファイル名をスキップ

    if latest_backup:
        restore_result = restore_database(latest_backup)
        logger.info(restore_result)
        assert "復元しました" in restore_result
    else:
        logger.info("バックアップファイルが見つからなかったため、復元テストをスキップします。")

    # クリーンアップ
    if DATABASE_FILE.exists():
        DATABASE_FILE.unlink()
    if BACKUP_DIR.exists():
        shutil.rmtree(BACKUP_DIR)
    logger.info("\nテスト完了。作成されたファイルとディレクトリをクリーンアップしました。")
