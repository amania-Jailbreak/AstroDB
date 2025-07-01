
import ujson
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Any
import logging
import asyncio

from contextlib import asynccontextmanager

# ロガーの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# プロジェクトのモジュールをインポート
import auth_engine
import automation_engine # automation_engineをインポート
from database import db_instance # データベースのシングルトンインスタンス

async def save_database_periodically(interval_minutes: int):
    while True:
        await asyncio.sleep(interval_minutes * 60)
        logger.info(f"定期保存: {interval_minutes} 分が経過しました。データベースを保存します。")
        db_instance.save_to_disk()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # アプリケーション起動時の処理
    logger.info("サーバーが起動しました。")
    # 定期保存タスクを開始
    save_task = asyncio.create_task(save_database_periodically(interval_minutes=1)) # 1分ごとに保存
    try:
        yield
    finally:
        # アプリケーション終了時の処理
        logger.info("シャットダウン処理を開始します...")
        save_task.cancel() # 定期保存タスクをキャンセル
        try:
            await save_task # タスクが終了するのを待つ
        except asyncio.CancelledError:
            logger.info("定期保存タスクがキャンセルされました。")
        db_instance.save_to_disk() # 最終保存
        logger.info("シャットダウン処理が完了しました。")

<<<<<<< HEAD
        # ロギングリスナーを停止
        listener.stop()

=======
>>>>>>> parent of a327377 (feat: 非同期ロギングを実装)
app = FastAPI(lifespan=lifespan)

async def handle_command(websocket: WebSocket, data: dict) -> dict:
    """受信したコマンドを解析し、適切なエンジンに処理を振り分ける"""
    command = data.get("command")
    response = {"status": "error", "message": "不明なコマンドです。"}

    try:
        # --- 認証が不要なコマンド ---
        if command == "REGISTER":
            success = auth_engine.register_user(data.get("username"), data.get("password"))
            if success:
                response = {"status": "ok", "message": "ユーザー登録が成功しました。"}
            else:
                response = {"status": "error", "message": "ユーザーが既に存在します。"}
            return response

        if command == "LOGIN":
            user = auth_engine.authenticate_user(data.get("username"), data.get("password"))
            if user:
                token = auth_engine.create_access_token({"sub": user["username"], "role": user["role"]})
                response = {"status": "ok", "token": token}
            else:
                response = {"status": "error", "message": "ユーザー名またはパスワードが不正です。"}
            return response

        # --- ここから先は認証が必要なコマンド ---
        token = data.get("token")
        if not token:
            return {"status": "error", "message": "認証トークンが必要です。"}

        user_payload = auth_engine.decode_access_token(token)
        if not user_payload:
            return {"status": "error", "message": "トークンが無効または期限切れです。"}
        
        owner_id = user_payload.get("sub") # トークンのsubject（ユーザー名）を所有者IDとする

        if command == "INSERT_ONE":
            collection = data.get("collection")
            document = data.get("document")
            if not isinstance(collection, str) or not collection:
                return {"status": "error", "message": "collectionは必須の文字列です。"}
            if not isinstance(document, dict) or not document:
                return {"status": "error", "message": "documentは必須の辞書です。"}

            # 権限チェック: ドキュメントにowner_idが指定されている場合、認証されたユーザーと一致するか確認
            if "owner_id" in document and document["owner_id"] != owner_id:
                return {"status": "error", "message": "他のユーザーのowner_idを持つドキュメントは挿入できません。"}
            
            # ドキュメントにowner_idが指定されていない場合、認証されたユーザーのowner_idを設定
            if "owner_id" not in document:
                document["owner_id"] = owner_id

            inserted_doc = db_instance.insert_one(collection, document, owner_id=owner_id)
            response = {"status": "ok", "data": inserted_doc}
            return response

        if command == "INSERT_MANY":
            collection = data.get("collection")
            documents = data.get("documents")
            if not isinstance(collection, str) or not collection:
                return {"status": "error", "message": "collectionは必須の文字列です。"}
            if not isinstance(documents, list) or not documents:
                return {"status": "error", "message": "documentsは必須のリストです。"}
            
            # 各ドキュメントにowner_idを設定
            for doc in documents:
                if "owner_id" in doc and doc["owner_id"] != owner_id:
                    return {"status": "error", "message": "他のユーザーのowner_idを持つドキュメントは挿入できません。"}
                if "owner_id" not in doc:
                    doc["owner_id"] = owner_id

            inserted_docs = db_instance.insert_many(collection, documents, owner_id=owner_id)
            response = {"status": "ok", "data": inserted_docs}
            return response

        if command == "FIND":
            collection = data.get("collection")
            query = data.get("query", {})
            if not isinstance(collection, str) or not collection:
                return {"status": "error", "message": "collectionは必須の文字列です。"}
            if not isinstance(query, dict):
                return {"status": "error", "message": "queryは辞書である必要があります。"}

            found_docs = db_instance.find(collection, query, owner_id=owner_id)
            response = {"status": "ok", "data": found_docs}
            return response

        if command == "UPDATE_ONE":
            collection = data.get("collection")
            query = data.get("query", {})
            update_data = data.get("update_data", {})
            if not isinstance(collection, str) or not collection:
                return {"status": "error", "message": "collectionは必須の文字列です。"}
            if not isinstance(query, dict):
                return {"status": "error", "message": "queryは辞書である必要があります。"}
            if not isinstance(update_data, dict) or not update_data:
                return {"status": "error", "message": "update_dataは必須の辞書です。"}

            updated_doc = db_instance.update_one(collection, query, update_data, owner_id=owner_id)
            if updated_doc:
                response = {"status": "ok", "data": updated_doc}
            else:
                response = {"status": "error", "message": "更新対象のドキュメントが見つからないか、権限がありません。"}
            return response

        if command == "UPDATE_MANY":
            collection = data.get("collection")
            query = data.get("query", {})
            update_data = data.get("update_data", {})
            if not isinstance(collection, str) or not collection:
                return {"status": "error", "message": "collectionは必須の文字列です。"}
            if not isinstance(query, dict):
                return {"status": "error", "message": "queryは辞書である必要があります。"}
            if not isinstance(update_data, dict) or not update_data:
                return {"status": "error", "message": "update_dataは必須の辞書です。"}

            updated_count = db_instance.update_many(collection, query, update_data, owner_id=owner_id)
            response = {"status": "ok", "updated_count": updated_count}
            return response

        if command == "DELETE_ONE":
            collection = data.get("collection")
            query = data.get("query", {})
            if not isinstance(collection, str) or not collection:
                return {"status": "error", "message": "collectionは必須の文字列です。"}
            if not isinstance(query, dict):
                return {"status": "error", "message": "queryは辞書である必要があります。"}

            deleted_doc = db_instance.delete_one(collection, query, owner_id=owner_id)
            if deleted_doc:
                response = {"status": "ok", "data": deleted_doc}
            else:
                response = {"status": "error", "message": "削除対象のドキュメントが見つからないか、権限がありません。"}
            return response

        if command == "DELETE_MANY":
            collection = data.get("collection")
            query = data.get("query", {})
            if not isinstance(collection, str) or not collection:
                return {"status": "error", "message": "collectionは必須の文字列です。"}
            if not isinstance(query, dict):
                return {"status": "error", "message": "queryは辞書である必要があります。"}

            deleted_count = db_instance.delete_many(collection, query, owner_id=owner_id)
            response = {"status": "ok", "deleted_count": deleted_count}
            return response

        if command == "FIND_ONE":
            collection = data.get("collection")
            query = data.get("query", {})
            if not isinstance(collection, str) or not collection:
                return {"status": "error", "message": "collectionは必須の文字列です。"}
            if not isinstance(query, dict):
                return {"status": "error", "message": "queryは辞書である必要があります。"}

            found_doc = db_instance.find_one(collection, query, owner_id=owner_id)
            if found_doc:
                response = {"status": "ok", "data": found_doc}
            else:
                response = {"status": "error", "message": "ドキュメントが見つからないか、権限がありません。"}
            return response

        if command == "FIND_MANY":
            collection = data.get("collection")
            query = data.get("query", {})
            if not isinstance(collection, str) or not collection:
                return {"status": "error", "message": "collectionは必須の文字列です。"}
            if not isinstance(query, dict):
                return {"status": "error", "message": "queryは辞書である必要があります。"}

            found_docs = db_instance.find_many(collection, query, owner_id=owner_id)
            response = {"status": "ok", "data": found_docs}
            return response

        if command == "BACKUP":
            # ロールベースのアクセス制御の例: adminロールのみがバックアップ可能
            if user_payload.get("role") == "admin":
                result_message = automation_engine.backup_database()
                response = {"status": "ok", "message": result_message}
            else:
                response = {"status": "error", "message": "権限がありません。"}
            return response

        if command == "RESTORE":
            # ロールベースのアクセス制御の例: adminロールのみが復元可能
            if user_payload.get("role") == "admin":
                backup_filename = data.get("filename")
                if backup_filename:
                    result_message = automation_engine.restore_database(backup_filename)
                    response = {"status": "ok", "message": result_message}
                else:
                    response = {"status": "error", "message": "バックアップファイル名が必要です。"}
            else:
                response = {"status": "error", "message": "権限がありません。"}
            return response

        if command == "CHANGE_PASSWORD":
            username = user_payload.get("sub") # 認証済みのユーザー名
            old_password = data.get("old_password")
            new_password = data.get("new_password")
            if username and old_password and new_password:
                success = auth_engine.change_password(username, old_password, new_password)
                if success:
                    response = {"status": "ok", "message": "パスワードが正常に変更されました。"}
                else:
                    response = {"status": "error", "message": "現在のパスワードが正しくありません。"}
            else:
                response = {"status": "error", "message": "old_passwordとnew_passwordが必要です。"}
            return response

        return response
    except Exception as e:
        logger.exception(f"handle_commandで予期せぬエラーが発生しました: {e}")
        return {"status": "error", "message": f"サーバー内部エラー: {e}"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger.info("クライアントが接続しました。")
    try:
        while True:
            raw_data = await websocket.receive_text()
            try:
                data = ujson.loads(raw_data)
                response = await handle_command(websocket, data)
            except (ValueError, TypeError) as e:
                response = {"status": "error", "message": "無効なJSON形式です。"}
                logger.error(f"無効なJSON形式を受信しました: {raw_data}, エラー: {e}")
            except Exception as e:
                response = {"status": "error", "message": "サーバー内部でエラーが発生しました。"}
                logger.exception(f"handle_commandの実行中に予期せぬエラーが発生しました: {e}")
            
            await websocket.send_text(ujson.dumps(response))

    except WebSocketDisconnect:
        logger.info("クライアントが切断しました。")
    except Exception as e:
        logger.exception(f"予期せぬエラーが発生しました: {e}")
        # クライアントにエラーを通知しようと試みる (接続がまだ生きていれば)
        try:
            await websocket.send_text(ujson.dumps({"status": "error", "message": "サーバー内部でエラーが発生しました。"}))
        except Exception:
            pass # 接続が既に切れている場合は何もしない



if __name__ == "__main__":
    import uvicorn
    logger.info("--- AstroDBサーバーを起動します ---")
    logger.info("URL: http://127.0.0.1:8000")
    logger.info("WebSocketエンドポイント: ws://127.0.0.1:8000/ws")
    logger.info("Ctrl+Cでサーバーを停止します。")
    uvicorn.run(app, host="127.0.0.1", port=8000)
