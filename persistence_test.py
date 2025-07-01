import time
import random
import string
import logging
import asyncio
import websockets
import ujson

# ロガーの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# サーバーURL
SERVER_URL = "ws://127.0.0.1:8000/ws"

# テスト用コレクション名
TEST_COLLECTION = "persistence_test_collection"
# テスト用ユーザー名とパスワード
TEST_USERNAME = "persistence_user"
TEST_PASSWORD = "persistence_password"

def generate_random_string(length=10):
    """指定された長さのランダムな文字列を生成する"""
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

class AstroDBClient:
    def __init__(self):
        self.websocket = None
        self.token = None

    async def connect(self):
        retries = 5
        for i in range(retries):
            try:
                self.websocket = await websockets.connect(SERVER_URL)
                logger.info("WebSocketに接続しました。")
                return
            except (websockets.exceptions.ConnectionClosedOK, ConnectionRefusedError) as e:
                logger.warning(f"接続試行 {i+1}/{retries} に失敗しました: {e}")
                await asyncio.sleep(1) # 1秒待機してからリトライ
        raise Exception(f"WebSocket接続に失敗しました。{retries} 回リトライしましたが接続できませんでした。")

    async def disconnect(self):
        if self.websocket:
            await self.websocket.close()
            logger.info("WebSocketから切断しました。")

    async def send_command(self, command: dict) -> dict:
        if self.token and "token" not in command:
            command["token"] = self.token
        
        await self.websocket.send(ujson.dumps(command))
        response_raw = await self.websocket.recv()
        response = ujson.loads(response_raw)
        return response

async def insert_test_data(client: AstroDBClient, num_documents: int):
    logger.info(f"--- {num_documents} 件のテストデータ挿入を開始します ---")
    
    # ユーザー登録とログイン
    # persistence_test.py はクライアントとして動作するため、サーバー側のauth_engineを直接呼び出すのではなく
    # WebSocket経由でREGISTERとLOGINコマンドを送信する
    response = await client.send_command({"command": "REGISTER", "username": TEST_USERNAME, "password": TEST_PASSWORD})
    if response.get("status") == "ok":
        logger.info("テストユーザーを登録しました。")
    elif response.get("message") == "ユーザーが既に存在します。":
        logger.info("テストユーザーは既に存在します。")
    else:
        logger.error(f"ユーザー登録に失敗しました: {response}")
        raise Exception("ユーザー登録失敗")

    response = await client.send_command({"command": "LOGIN", "username": TEST_USERNAME, "password": TEST_PASSWORD})
    if response.get("status") == "ok" and response.get("token"):
        client.token = response["token"]
        logger.info("テストユーザーとしてログインしました。")
    else:
        logger.error(f"ログインに失敗しました: {response}")
        raise Exception("ログイン失敗")

    documents = []
    for i in range(num_documents):
        documents.append({
            "name": generate_random_string(15),
            "value": random.randint(1, 10000),
            "timestamp": time.time(),
            "test_id": f"test_doc_{i}" # 永続性テスト用ID
        })

    for doc in documents:
        response = await client.send_command({"command": "INSERT_ONE", "collection": TEST_COLLECTION, "document": doc})
        assert response.get("status") == "ok", f"挿入失敗: {response}"
    logger.info(f"{num_documents} 件のテストデータ挿入が完了しました。")

async def main():
    logger.info("=====================================")
    logger.info("  AstroDB 永続性テスト - データ挿入")
    logger.info("=====================================")

    NUM_DOCUMENTS_TO_INSERT = 5 # 挿入するドキュメント数

    client = AstroDBClient()
    try:
        await client.connect()
        await insert_test_data(client, NUM_DOCUMENTS_TO_INSERT)
    except Exception as e:
        logger.error(f"データ挿入中にエラーが発生しました: {e}")
    finally:
        await client.disconnect()
        logger.info("=====================================")
        logger.info("  AstroDB 永続性テスト - データ挿入終了")
        logger.info("=====================================")

if __name__ == "__main__":
    asyncio.run(main())
