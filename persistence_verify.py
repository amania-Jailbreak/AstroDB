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
TEST_USERNAME = "persistence_user_test"
TEST_PASSWORD = "persistence_password_test"

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

async def verify_test_data(client: AstroDBClient, num_documents: int):
    logger.info(f"--- {num_documents} 件のテストデータ検索を開始します ---")
    
    # ユーザーログイン
    response = await client.send_command({"command": "LOGIN", "username": TEST_USERNAME, "password": TEST_PASSWORD})
    if response.get("status") == "ok" and response.get("token"):
        client.token = response["token"]
        logger.info("テストユーザーとしてログインしました。")
    else:
        logger.error(f"ログインに失敗しました: {response}")
        raise Exception("ログイン失敗")

    found_count = 0
    for i in range(num_documents):
        query = {"test_id": f"test_doc_{i}"}
        response = await client.send_command({"command": "FIND_ONE", "collection": TEST_COLLECTION, "query": query})
        if response.get("status") == "ok" and response["data"] is not None:
            logger.info(f"ドキュメントが見つかりました: {response["data"]["test_id"]}")
            found_count += 1
        else:
            logger.error(f"ドキュメントが見つかりませんでした: {query} - {response}")

    if found_count == num_documents:
        logger.info(f"すべての {num_documents} 件のドキュメントが正常に検索されました。")
        return True
    else:
        logger.error(f"期待したドキュメント数 ({num_documents}) と異なる結果 ({found_count}) です。")
        return False

async def main():
    logger.info("=====================================")
    logger.info("  AstroDB 永続性テスト - データ検証")
    logger.info("=====================================")

    NUM_DOCUMENTS_TO_VERIFY = 5 # 挿入したドキュメント数と同じ

    client = AstroDBClient()
    try:
        await client.connect()
        success = await verify_test_data(client, NUM_DOCUMENTS_TO_VERIFY)
        if success:
            logger.info("永続性テスト成功！")
        else:
            logger.error("永続性テスト失敗！")
    except Exception as e:
        logger.error(f"データ検証中にエラーが発生しました: {e}")
    finally:
        await client.disconnect()
        logger.info("=====================================")
        logger.info("  AstroDB 永続性テスト - データ検証終了")
        logger.info("=====================================")

if __name__ == "__main__":
    asyncio.run(main())
