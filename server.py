
import ujson
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Any

# プロジェクトのモジュールをインポート
import auth_engine
from database import db_instance # データベースのシングルトンインスタンス

app = FastAPI()

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
            # TODO: 権限チェック (例: このコレクションに書き込み可能か)
            inserted_doc = db_instance.insert_one(collection, document, owner_id=owner_id)
            response = {"status": "ok", "data": inserted_doc}
        else:
            response = {"status": "error", "message": "collectionとdocumentが必要です。"}
        return response

    # 他のコマンド (FIND, UPDATE, DELETEなど) はここに追加していく

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

@app.on_event("shutdown")
def shutdown_event():
    """アプリケーション終了時にデータベースを保存する"""
    print("シャットダウン処理を開始します...")
    db_instance.save_to_disk()
    print("シャットダウン処理が完了しました。")

if __name__ == "__main__":
    import uvicorn
    print("--- AstroDBサーバーを起動します ---")
    print("URL: http://127.0.0.1:8000")
    print("WebSocketエンドポイント: ws://127.0.0.1:8000/ws")
    print("Ctrl+Cでサーバーを停止します。")
    uvicorn.run(app, host="127.0.0.1", port=8000)
