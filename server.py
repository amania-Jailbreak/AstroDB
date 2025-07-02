import ujson
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Any
import logging
import asyncio

from contextlib import asynccontextmanager

# ロガーの設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Error Codes
ERROR_UNKNOWN_COMMAND = "UNKNOWN_COMMAND"
ERROR_USER_ALREADY_EXISTS = "USER_ALREADY_EXISTS"
ERROR_INVALID_CREDENTIALS = "INVALID_CREDENTIALS"
ERROR_AUTH_TOKEN_REQUIRED = "AUTH_TOKEN_REQUIRED"
ERROR_INVALID_TOKEN = "INVALID_TOKEN"
ERROR_COLLECTION_REQUIRED = "COLLECTION_REQUIRED"
ERROR_DOCUMENT_REQUIRED = "DOCUMENT_REQUIRED"
ERROR_INVALID_OWNER_ID = "INVALID_OWNER_ID"
ERROR_QUERY_REQUIRED = "QUERY_REQUIRED"
ERROR_UPDATE_DATA_REQUIRED = "UPDATE_DATA_REQUIRED"
ERROR_DOC_NOT_FOUND_OR_PERMISSION_DENIED = "DOC_NOT_FOUND_OR_PERMISSION_DENIED"
ERROR_FIELD_REQUIRED = "FIELD_REQUIRED"
ERROR_PERMISSION_DENIED = "PERMISSION_DENIED"
ERROR_BACKUP_FILENAME_REQUIRED = "BACKUP_FILENAME_REQUIRED"
ERROR_INCORRECT_CURRENT_PASSWORD = "INCORRECT_CURRENT_PASSWORD"
ERROR_PASSWORD_FIELDS_REQUIRED = "PASSWORD_FIELDS_REQUIRED"
ERROR_INTERNAL_SERVER_ERROR = "INTERNAL_SERVER_ERROR"
ERROR_INVALID_JSON_FORMAT = "INVALID_JSON_FORMAT"

# プロジェクトのモジュールをインポート
import auth_engine
import automation_engine  # automation_engineをインポート
from database import db_instance  # データベースのシングルトンインスタンス


async def save_database_periodically(interval_minutes: int):
    while True:
        await asyncio.sleep(interval_minutes * 60)
        logger.info(
            f"定期保存: {interval_minutes} 分が経過しました。データベースを保存します。"
        )
        db_instance.save_to_disk()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # アプリケーション起動時の処理
    logger.info("サーバーが起動しました。")
    # 定期保存タスクを開始
    save_task = asyncio.create_task(
        save_database_periodically(interval_minutes=1)
    )  # 1分ごとに保存
    try:
        yield
    finally:
        # アプリケーション終了時の処理
        logger.info("シャットダウン処理を開始します...")
        save_task.cancel()  # 定期保存タスクをキャンセル
        try:
            await save_task  # タスクが終了するのを待つ
        except asyncio.CancelledError:
            logger.info("定期保存タスクがキャンセルされました。")
        db_instance.save_to_disk()  # 最終保存
        logger.info("シャットダウン処理が完了しました。")


app = FastAPI(lifespan=lifespan)


async def handle_command(websocket: WebSocket, data: dict) -> dict:
    """受信したコマンドを解析し、適切なエンジンに処理を振り分ける"""
    command = data.get("command")
    response = {"status": "error", "message": "Unknown command.", "code": ERROR_UNKNOWN_COMMAND}

    try:
        # --- 認証が不要なコマンド ---
        logger.info(f"Running command: {command}")
        if command == "REGISTER":
            success = auth_engine.register_user(
                data.get("username"), data.get("password")
            )
            if success:
                response = {"status": "ok", "message": "User registration successful."}
            else:
                response = {"status": "error", "message": "User already exists.", "code": ERROR_USER_ALREADY_EXISTS}
            return response

        if command == "LOGIN":
            user = auth_engine.authenticate_user(
                data.get("username"), data.get("password")
            )
            if user:
                token = auth_engine.create_access_token(
                    {"sub": user["username"], "role": user["role"]}
                )
                response = {"status": "ok", "token": token}
            else:
                response = {
                    "status": "error",
                    "message": "Invalid username or password.",
                    "code": ERROR_INVALID_CREDENTIALS,
                }
            return response

        # --- ここから先は認証が必要なコマンド ---
        token = data.get("token")
        if not token:
            return {"status": "error", "message": "Authentication token is required.", "code": ERROR_AUTH_TOKEN_REQUIRED}

        user_payload = auth_engine.decode_access_token(token)
        if not user_payload:
            return {"status": "error", "message": "Invalid or expired token.", "code": ERROR_INVALID_TOKEN}

        owner_id = user_payload.get(
            "sub"
        )  # トークンのsubject（ユーザー名）を所有者IDとする
        logger.info(f"USER: {owner_id}")

        if command == "INSERT_ONE":
            collection = data.get("collection")
            document = data.get("document")
            if not isinstance(collection, str) or not collection:
                return {"status": "error", "message": "Collection must be a non-empty string.", "code": ERROR_COLLECTION_REQUIRED}
            if not isinstance(document, dict) or not document:
                return {"status": "error", "message": "Document must be a non-empty dictionary.", "code": ERROR_DOCUMENT_REQUIRED}

            # Permission check: If owner_id is specified in the document, ensure it matches the authenticated user
            if "owner_id" in document and document["owner_id"] != owner_id:
                return {
                    "status": "error",
                    "message": "Cannot insert documents with another user's owner_id.",
                    "code": ERROR_INVALID_OWNER_ID,
                }

            # ドキュメントにowner_idが指定されていない場合、認証されたユーザーのowner_idを設定
            if "owner_id" not in document:
                document["owner_id"] = owner_id

            inserted_doc = db_instance.insert_one(
                collection, document, owner_id=owner_id
            )
            response = {"status": "ok", "data": inserted_doc}
            return response

        if command == "INSERT_MANY":
            collection = data.get("collection")
            documents = data.get("documents")
            if not isinstance(collection, str) or not collection:
                return {
                    "status": "error",
                    "message": "Collection must be a non-empty string.",
                }
            if not isinstance(documents, list) or not documents:
                return {
                    "status": "error",
                    "message": "Documents must be a non-empty list.",
                }

            # Set owner_id for each document
            for doc in documents:
                if "owner_id" in doc and doc["owner_id"] != owner_id:
                    return {
                        "status": "error",
                        "message": "Cannot insert documents with another user's owner_id.",
                        "code": ERROR_INVALID_OWNER_ID,
                    }
                if "owner_id" not in doc:
                    doc["owner_id"] = owner_id

            inserted_docs = db_instance.insert_many(
                collection, documents, owner_id=owner_id
            )
            response = {"status": "ok", "data": inserted_docs}
            return response

        if command == "FIND":
            collection = data.get("collection")
            query = data.get("query", {})
            if not isinstance(collection, str) or not collection:
                return {
                    "status": "error",
                    "message": "Collection must be a non-empty string.",
                    "code": ERROR_COLLECTION_REQUIRED,
                }
            if not isinstance(query, dict):
                return {
                    "status": "error",
                    "message": "Query must be a dictionary.",
                    "code": ERROR_QUERY_REQUIRED,
                }

            found_docs = db_instance.find(collection, query, owner_id=owner_id)
            response = {"status": "ok", "data": found_docs}
            return response

        if command == "UPDATE_ONE":
            collection = data.get("collection")
            query = data.get("query", {})
            update_data = data.get("update_data", {})
            if not isinstance(collection, str) or not collection:
                return {
                    "status": "error",
                    "message": "Collection must be a non-empty string.",
                    "code": ERROR_COLLECTION_REQUIRED,
                }
            if not isinstance(query, dict):
                return {
                    "status": "error",
                    "message": "Query must be a dictionary.",
                    "code": ERROR_QUERY_REQUIRED,
                }
            if not isinstance(update_data, dict) or not update_data:
                return {
                    "status": "error",
                    "message": "Update data must be a non-empty dictionary.",
                    "code": ERROR_UPDATE_DATA_REQUIRED,
                }

            updated_doc = db_instance.update_one(
                collection, query, update_data, owner_id=owner_id
            )
            if updated_doc:
                response = {"status": "ok", "data": updated_doc}
            else:
                response = {
                    "status": "error",
                    "message": "Document to update not found or insufficient permissions.",
                    "code": ERROR_DOC_NOT_FOUND_OR_PERMISSION_DENIED,
                }
            return response

        if command == "UPDATE_MANY":
            collection = data.get("collection")
            query = data.get("query", {})
            update_data = data.get("update_data", {})
            if not isinstance(collection, str) or not collection:
                return {
                    "status": "error",
                    "message": "Collection must be a non-empty string.",
                    "code": ERROR_COLLECTION_REQUIRED,
                }
            if not isinstance(query, dict):
                return {
                    "status": "error",
                    "message": "Query must be a dictionary.",
                    "code": ERROR_QUERY_REQUIRED,
                }
            if not isinstance(update_data, dict) or not update_data:
                return {
                    "status": "error",
                    "message": "Update data must be a non-empty dictionary.",
                    "code": ERROR_UPDATE_DATA_REQUIRED,
                }

            updated_count = db_instance.update_many(
                collection, query, update_data, owner_id=owner_id
            )
            response = {"status": "ok", "updated_count": updated_count}
            return response

        if command == "DELETE_ONE":
            collection = data.get("collection")
            query = data.get("query", {})
            if not isinstance(collection, str) or not collection:
                return {
                    "status": "error",
                    "message": "Collection must be a non-empty string.",
                    "code": ERROR_COLLECTION_REQUIRED,
                }
            if not isinstance(query, dict):
                return {
                    "status": "error",
                    "message": "Query must be a dictionary.",
                    "code": ERROR_QUERY_REQUIRED,
                }

            deleted_doc = db_instance.delete_one(collection, query, owner_id=owner_id)
            if deleted_doc:
                response = {"status": "ok", "data": deleted_doc}
            else:
                response = {
                    "status": "error",
                    "message": "Document to delete not found or insufficient permissions.",
                    "code": ERROR_DOC_NOT_FOUND_OR_PERMISSION_DENIED,
                }
            return response

        if command == "DELETE_MANY":
            collection = data.get("collection")
            query = data.get("query", {})
            if not isinstance(collection, str) or not collection:
                return {
                    "status": "error",
                    "message": "Collection must be a non-empty string.",
                }
            if not isinstance(query, dict):
                return {
                    "status": "error",
                    "message": "Query must be a dictionary.",
                }

            deleted_count = db_instance.delete_many(
                collection, query, owner_id=owner_id
            )
            response = {"status": "ok", "deleted_count": deleted_count}
            return response

        if command == "FIND_ONE":
            collection = data.get("collection")
            query = data.get("query", {})
            if not isinstance(collection, str) or not collection:
                return {
                    "status": "error",
                    "message": "Collection must be a non-empty string.",
                }
            if not isinstance(query, dict):
                return {
                    "status": "error",
                    "message": "Query must be a dictionary.",
                }

            found_doc = db_instance.find_one(collection, query, owner_id=owner_id)
            if found_doc:
                response = {"status": "ok", "data": found_doc}
            else:
                response = {
                    "status": "error",
                    "message": "Document not found or insufficient permissions.",
                }
            return response

        if command == "FIND_MANY":
            collection = data.get("collection")
            query = data.get("query", {})
            if not isinstance(collection, str) or not collection:
                return {
                    "status": "error",
                    "message": "Collection must be a non-empty string.",
                }
            if not isinstance(query, dict):
                return {
                    "status": "error",
                    "message": "Query must be a dictionary.",
                }

            found_docs = db_instance.find_many(collection, query, owner_id=owner_id)
            response = {"status": "ok", "data": found_docs}
            return response

        if command == "CREATE_INDEX":
            collection = data.get("collection")
            field = data.get("field")
            if not isinstance(collection, str) or not collection:
                return {
                    "status": "error",
                    "message": "Collection must be a non-empty string.",
                }
            if not isinstance(field, str) or not field:
                return {
                    "status": "error",
                    "message": "Field must be a non-empty string.",
                }

            db_instance.create_index(collection, field)
            response = {
                "status": "ok",
                "message": f"Index created on collection '{collection}', field '{field}'.",
            }
            return response

        if command == "BACKUP":
            # Role-based access control example: Only admin role can backup
            if user_payload.get("role") == "admin":
                result_message = automation_engine.backup_database()
                response = {"status": "ok", "message": result_message}
            else:
                response = {"status": "error", "message": "Permission denied."}
            return response

        if command == "RESTORE":
            # Role-based access control example: Only admin role can restore
            if user_payload.get("role") == "admin":
                backup_filename = data.get("filename")
                if backup_filename:
                    result_message = automation_engine.restore_database(backup_filename)
                    response = {"status": "ok", "message": result_message}
                else:
                    response = {
                        "status": "error",
                        "message": "Backup filename is required.",
                    }
            else:
                response = {"status": "error", "message": "Permission denied."}
            return response

        if command == "CHANGE_PASSWORD":
            username = user_payload.get("sub")  # Authenticated username
            old_password = data.get("old_password")
            new_password = data.get("new_password")
            if username and old_password and new_password:
                success = auth_engine.change_password(
                    username, old_password, new_password
                )
                if success:
                    response = {
                        "status": "ok",
                        "message": "Password changed successfully.",
                    }
                else:
                    response = {
                        "status": "error",
                        "message": "Current password is incorrect.",
                    }
            else:
                response = {
                    "status": "error",
                    "message": "Old password and new password are required.",
                }
            return response

        return response
    except Exception as e:
        logger.exception(f"An unexpected error occurred in handle_command: {e}")
        return {"status": "error", "message": f"Internal server error: {e}"}


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
                response = {"status": "error", "message": "Invalid JSON format."}
                logger.error(f"Received invalid JSON format: {raw_data}, Error: {e}")
            except Exception as e:
                response = {
                    "status": "error",
                    "message": "An internal server error occurred.",
                }
                logger.exception(
                    f"An unexpected error occurred during handle_command execution: {e}"
                )

            await websocket.send_text(ujson.dumps(response))

    except WebSocketDisconnect:
        logger.info("クライアントが切断しました。")
    except Exception as e:
        logger.exception(f"An unexpected error occurred: {e}")
        # Attempt to notify the client of the error (if connection is still alive)
        try:
            await websocket.send_text(
                ujson.dumps(
                    {
                        "status": "error",
                        "message": "An internal server error occurred.",
                    }
                )
            )
        except Exception:
            pass  # Do nothing if the connection is already closed


if __name__ == "__main__":
    import uvicorn

    logger.info("--- AstroDBサーバーを起動します ---")
    logger.info("URL: http://127.0.0.1:8000")
    logger.info("WebSocketエンドポイント: ws://127.0.0.1:8000/ws")
    logger.info("Ctrl+Cでサーバーを停止します。")
    uvicorn.run(app, host="127.0.0.1", port=8000)
