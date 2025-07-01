
import ujson
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Any

from contextlib import asynccontextmanager

# プロジェクトのモジュールをインポート
import auth_engine
from database import db_instance # データベースのシングルトンインスタンス

@asynccontextmanager
async def lifespan(app: FastAPI):
    # アプリケーション起動時の処理
    # （今回は特にないが、将来的にDB接続プールなどを作成する場合はここに書く）
    print("サーバーが起動しました。")
    yield
    # アプリケーション終了時の処理
    print("シャットダウン処理を開始します...")
    db_instance.save_to_disk()
    print("シャットダウン処理が完了しました。")

app = FastAPI(lifespan=lifespan)

async def handle_command(websocket: WebSocket, data: dict) -> dict:
    """受信したコマンドを解析し、適切なエンジンに処理を振り分ける"""
    command = data.get("command")
    response = {"status": "error", "message": "不明なコマンドです。"}

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
        if collection and document:
            # 権限チェック: ドキュメントにowner_idが指定されている場合、認証されたユーザーと一致するか確認
            if "owner_id" in document and document["owner_id"] != owner_id:
                return {"status": "error", "message": "他のユーザーのowner_idを持つドキュメントは挿入できません。"}
            
            # ドキュメントにowner_idが指定されていない場合、認証されたユーザーのowner_idを設定
            if "owner_id" not in document:
                document["owner_id"] = owner_id

            inserted_doc = db_instance.insert_one(collection, document, owner_id=owner_id)
            response = {"status": "ok", "data": inserted_doc}
        else:
            response = {"status": "error", "message": "collectionとdocumentが必要です。"}
        return response

    if command == "FIND":
        collection = data.get("collection")
        query = data.get("query", {})
        if collection:
            found_docs = db_instance.find(collection, query, owner_id=owner_id)
            response = {"status": "ok", "data": found_docs}
        else:
            response = {"status": "error", "message": "collectionが必要です。"}
        return response

    # 他のコマンド (UPDATE, DELETEなど) はここに追加していく

    return response

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    print("クライアントが接続しました。")
    try:
        while True:
            raw_data = await websocket.receive_text()
            try:
                data = ujson.loads(raw_data)
                response = await handle_command(websocket, data)
            except (ValueError, TypeError):
                response = {"status": "error", "message": "無効なJSON形式です。"}
            
            await websocket.send_text(ujson.dumps(response))

    except WebSocketDisconnect:
        print("クライアントが切断しました。")
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
        # クライアントにエラーを通知しようと試みる (接続がまだ生きていれば)
        try:
            await websocket.send_text(ujson.dumps({"status": "error", "message": "サーバー内部でエラーが発生しました。"}))
        except Exception:
            pass # 接続が既に切れている場合は何もしない



if __name__ == "__main__":
    import uvicorn
    print("--- AstroDBサーバーを起動します ---")
    print("URL: http://127.0.0.1:8000")
    print("WebSocketエンドポイント: ws://127.0.0.1:8000/ws")
    print("Ctrl+Cでサーバーを停止します。")
    uvicorn.run(app, host="127.0.0.1", port=8000)
