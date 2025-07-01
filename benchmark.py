import time
import random
import string
import logging
import asyncio
import websockets
import ujson

# ロガーの設定
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# サーバーURL
SERVER_URL = "ws://127.0.0.1:8000/ws"

# テスト用コレクション名
TEST_COLLECTION = "benchmark_collection"
# テスト用ユーザー名とパスワード
TEST_USERNAME = "benchmark_user"
TEST_PASSWORD = "benchmark_password"


def generate_random_string(length=10):
    """指定された長さのランダムな文字列を生成する"""
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(length))


class AstroDBClient:
    def __init__(self):
        self.websocket = None
        self.token = None

    async def connect(self):
        self.websocket = await websockets.connect(SERVER_URL)
        logger.info("WebSocketに接続しました。")

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


async def setup_benchmark_db(client: AstroDBClient):
    """ベンチマーク用のデータベースをセットアップする"""
    logger.info("ベンチマークデータベースのセットアップを開始します...")

    # ユーザー登録とログイン
    response = await client.send_command(
        {"command": "REGISTER", "username": TEST_USERNAME, "password": TEST_PASSWORD}
    )
    if response.get("status") == "ok":
        logger.info("ベンチマークユーザーを登録しました。")
    elif response.get("message") == "ユーザーが既に存在します。":
        logger.info("ベンチマークユーザーは既に存在します。")
    else:
        logger.error(f"ユーザー登録に失敗しました: {response}")
        raise Exception("ユーザー登録失敗")

    response = await client.send_command(
        {"command": "LOGIN", "username": TEST_USERNAME, "password": TEST_PASSWORD}
    )
    if response.get("status") == "ok" and response.get("token"):
        client.token = response["token"]
        logger.info("ベンチマークユーザーとしてログインしました。")
    else:
        logger.error(f"ログインに失敗しました: {response}")
        raise Exception("ログイン失敗")

    logger.info("ベンチマークデータベースのセットアップが完了しました。")


async def teardown_benchmark_db(client: AstroDBClient):
    """ベンチマーク用のデータベースをクリーンアップする"""
    logger.info("ベンチマークデータベースのクリーンアップを開始します...")
    # データベースファイルを直接削除する代わりに、サーバー経由でコレクションをクリアする
    # ただし、これはテストコレクションのみを対象とし、ユーザーデータは残す
    # 実際のクリーンアップは手動で行うか、専用のAPIを設けるべき
    # ここでは、テストで挿入したドキュメントを削除する
    await client.send_command(
        {"command": "DELETE_MANY", "collection": TEST_COLLECTION, "query": {}}
    )
    logger.info(f"コレクション '{TEST_COLLECTION}' のドキュメントを削除しました。")
    logger.info("ベンチマークデータベースのクリーンアップが完了しました。")


async def benchmark_insert_one(client: AstroDBClient, num_documents: int):
    """単一ドキュメント挿入のベンチマーク"""
    logger.info(f"--- {num_documents} 件のドキュメント挿入ベンチマークを開始します ---")
    documents = []
    for i in range(num_documents):
        documents.append(
            {
                "name": generate_random_string(15),
                "value": random.randint(1, 10000),
                "timestamp": time.time(),
                "index_field": f"idx_{i}",  # インデックス用フィールド
            }
        )

    start_time = time.perf_counter()
    for doc in documents:
        response = await client.send_command(
            {"command": "INSERT_ONE", "collection": TEST_COLLECTION, "document": doc}
        )
        assert response.get("status") == "ok", f"挿入失敗: {response}"
    end_time = time.perf_counter()

    elapsed_time = end_time - start_time
    avg_time_per_doc = elapsed_time / num_documents
    logger.info(f"挿入完了。合計時間: {elapsed_time:.4f} 秒")
    logger.info(f"1 ドキュメントあたりの平均挿入時間: {avg_time_per_doc:.6f} 秒")
    logger.info(f"スループット: {num_documents / elapsed_time:.2f} ドキュメント/秒")
    return elapsed_time


async def benchmark_find_one(client: AstroDBClient, num_queries: int):
    """単一ドキュメント検索 (find_one) のベンチマーク"""
    logger.info(
        f"--- {num_queries} 回の単一ドキュメント検索ベンチマークを開始します ---"
    )

    # 検索対象のドキュメントを事前に挿入
    inserted_docs = []
    for i in range(num_queries):
        doc = {
            "name": generate_random_string(15),
            "value": random.randint(1, 10000),
            "timestamp": time.time(),
            "unique_id": f"query_{i}",  # ユニークなIDで検索
        }
        response = await client.send_command(
            {"command": "INSERT_ONE", "collection": TEST_COLLECTION, "document": doc}
        )
        assert response.get("status") == "ok", f"挿入失敗: {response}"
        inserted_docs.append(response["data"])

    # インデックスを作成 (もしあれば)
    # await client.send_command({"command": "CREATE_INDEX", "collection": TEST_COLLECTION, "field": "unique_id"}) # サーバー側で実装が必要

    start_time = time.perf_counter()
    for doc in inserted_docs:
        query = {"unique_id": doc["unique_id"]}
        response = await client.send_command(
            {"command": "FIND_ONE", "collection": TEST_COLLECTION, "query": query}
        )
        assert (
            response.get("status") == "ok"
            and response["data"] is not None
            and response["data"]["unique_id"] == doc["unique_id"]
        ), f"検索失敗: {response}"
    end_time = time.perf_counter()

    elapsed_time = end_time - start_time
    avg_time_per_query = elapsed_time / num_queries
    logger.info(f"検索完了。合計時間: {elapsed_time:.4f} 秒")
    logger.info(f"1 クエリあたりの平均検索時間: {avg_time_per_query:.6f} 秒")
    logger.info(f"スループット: {num_queries / elapsed_time:.2f} クエリ/秒")
    return elapsed_time


async def benchmark_update_one(client: AstroDBClient, num_updates: int):
    """単一ドキュメント更新 (update_one) のベンチマーク"""
    logger.info(
        f"--- {num_updates} 回の単一ドキュメント更新ベンチマークを開始します ---"
    )

    # 更新対象のドキュメントを事前に挿入
    inserted_docs = []
    for i in range(num_updates):
        doc = {
            "name": generate_random_string(15),
            "status": "pending",
            "update_id": f"update_{i}",  # ユニークなIDで更新
        }
        response = await client.send_command(
            {"command": "INSERT_ONE", "collection": TEST_COLLECTION, "document": doc}
        )
        assert response.get("status") == "ok", f"挿入失敗: {response}"
        inserted_docs.append(response["data"])

    # インデックスを作成 (もしあれば)
    # await client.send_command({"command": "CREATE_INDEX", "collection": TEST_COLLECTION, "field": "update_id"}) # サーバー側で実装が必要

    start_time = time.perf_counter()
    for doc in inserted_docs:
        query = {"update_id": doc["update_id"]}
        update_data = {"status": "completed", "updated_at": time.time()}
        response = await client.send_command(
            {
                "command": "UPDATE_ONE",
                "collection": TEST_COLLECTION,
                "query": query,
                "update_data": update_data,
            }
        )
        assert (
            response.get("status") == "ok"
            and response["data"] is not None
            and response["data"]["status"] == "completed"
        ), f"更新失敗: {response}"
    end_time = time.perf_counter()

    elapsed_time = end_time - start_time
    avg_time_per_update = elapsed_time / num_updates
    logger.info(f"更新完了。合計時間: {elapsed_time:.4f} 秒")
    logger.info(f"1 ドキュメントあたりの平均更新時間: {avg_time_per_update:.6f} 秒")
    logger.info(f"スループット: {num_updates / elapsed_time:.2f} 更新/秒")
    return elapsed_time


async def benchmark_delete_one(client: AstroDBClient, num_deletes: int):
    """単一ドキュメント削除 (delete_one) のベンチマーク"""
    logger.info(
        f"--- {num_deletes} 回の単一ドキュメント削除ベンチマークを開始します ---"
    )

    # 削除対象のドキュメントを事前に挿入
    inserted_docs = []
    for i in range(num_deletes):
        doc = {
            "name": generate_random_string(15),
            "delete_id": f"delete_{i}",  # ユニークなIDで削除
        }
        response = await client.send_command(
            {"command": "INSERT_ONE", "collection": TEST_COLLECTION, "document": doc}
        )
        assert response.get("status") == "ok", f"挿入失敗: {response}"
        inserted_docs.append(response["data"])

    # インデックスを作成 (もしあれば)
    # await client.send_command({"command": "CREATE_INDEX", "collection": TEST_COLLECTION, "field": "delete_id"}) # サーバー側で実装が必要

    start_time = time.perf_counter()
    for doc in inserted_docs:
        query = {"delete_id": doc["delete_id"]}
        response = await client.send_command(
            {"command": "DELETE_ONE", "collection": TEST_COLLECTION, "query": query}
        )
        assert (
            response.get("status") == "ok"
            and response["data"] is not None
            and response["data"]["delete_id"] == doc["delete_id"]
        ), f"削除失敗: {response}"
    end_time = time.perf_counter()

    elapsed_time = end_time - start_time
    avg_time_per_delete = elapsed_time / num_deletes
    logger.info(f"削除完了。合計時間: {elapsed_time:.4f} 秒")
    logger.info(f"1 ドキュメントあたりの平均削除時間: {avg_time_per_delete:.6f} 秒")
    logger.info(f"スループット: {num_deletes / elapsed_time:.2f} 削除/秒")
    return elapsed_time


async def main():
    logger.info("=====================================")
    logger.info("  AstroDB ベンチマークテスト開始")
    logger.info("=====================================")

    # ベンチマーク設定
    NUM_DOCUMENTS_INSERT = 1000
    NUM_QUERIES_FIND = 500
    NUM_UPDATES = 500
    NUM_DELETES = 500

    client = AstroDBClient()
    try:
        await client.connect()
        await setup_benchmark_db(client)

        # 挿入ベンチマーク
        insert_time = await benchmark_insert_one(client, NUM_DOCUMENTS_INSERT)

        # 検索ベンチマーク
        find_time = await benchmark_find_one(client, NUM_QUERIES_FIND)

        # 更新ベンチマーク
        update_time = await benchmark_update_one(client, NUM_UPDATES)

        # 削除ベンチマーク
        delete_time = await benchmark_delete_one(client, NUM_DELETES)

        logger.info("=====================================")
        logger.info("  AstroDB ベンチマークテスト結果")
        logger.info("=====================================")
        logger.info(f"挿入 ({NUM_DOCUMENTS_INSERT} 件): {insert_time:.4f} 秒")
        logger.info(f"検索 ({NUM_QUERIES_FIND} 回): {find_time:.4f} 秒")
        logger.info(f"更新 ({NUM_UPDATES} 回): {update_time:.4f} 秒")
        logger.info(f"削除 ({NUM_DELETES} 回): {delete_time:.4f} 秒")

    except Exception as e:
        logger.error(f"ベンチマーク中にエラーが発生しました: {e}")
    finally:
        await teardown_benchmark_db(client)
        await client.disconnect()
        logger.info("=====================================")
        logger.info("  AstroDB ベンチマークテスト終了")
        logger.info("=====================================")


if __name__ == "__main__":
    asyncio.run(main())
