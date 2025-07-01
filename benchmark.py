import time
import random
import string
import logging
from database import db_instance, DATABASE_FILE
import os

# ロガーの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# テスト用コレクション名
TEST_COLLECTION = "benchmark_collection"
# テスト用オーナーID
TEST_OWNER_ID = "benchmark_user"

def generate_random_string(length=10):
    """指定された長さのランダムな文字列を生成する"""
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def setup_benchmark_db():
    """ベンチマーク用のデータベースをセットアップする"""
    logger.info("ベンチマークデータベースのセットアップを開始します...")
    # 既存のデータベースファイルを削除してクリーンな状態にする
    if DATABASE_FILE.exists():
        DATABASE_FILE.unlink()
        logger.info(f"既存のデータベースファイル {DATABASE_FILE} を削除しました。")
    
    # 新しいデータベースインスタンスをロード（ファイルがなければ新規作成される）
    db_instance.load_from_disk()
    logger.info("ベンチマークデータベースのセットアップが完了しました。")

def teardown_benchmark_db():
    """ベンチマーク用のデータベースをクリーンアップする"""
    logger.info("ベンチマークデータベースのクリーンアップを開始します...")
    if DATABASE_FILE.exists():
        DATABASE_FILE.unlink()
        logger.info(f"データベースファイル {DATABASE_FILE} を削除しました。")
    logger.info("ベンチマークデータベースのクリーンアップが完了しました。")

def benchmark_insert_one(num_documents: int):
    """単一ドキュメント挿入のベンチマーク"""
    logger.info(f"--- {num_documents} 件のドキュメント挿入ベンチマークを開始します ---")
    documents = []
    for i in range(num_documents):
        documents.append({
            "name": generate_random_string(15),
            "value": random.randint(1, 10000),
            "timestamp": time.time(),
            "index_field": f"idx_{i}" # インデックス用フィールド
        })

    start_time = time.perf_counter()
    for doc in documents:
        db_instance.insert_one(TEST_COLLECTION, doc, TEST_OWNER_ID)
    end_time = time.perf_counter()
    
    elapsed_time = end_time - start_time
    avg_time_per_doc = elapsed_time / num_documents
    logger.info(f"挿入完了。合計時間: {elapsed_time:.4f} 秒")
    logger.info(f"1 ドキュメントあたりの平均挿入時間: {avg_time_per_doc:.6f} 秒")
    logger.info(f"スループット: {num_documents / elapsed_time:.2f} ドキュメント/秒")
    return elapsed_time

def benchmark_find_one(num_queries: int):
    """単一ドキュメント検索 (find_one) のベンチマーク"""
    logger.info(f"--- {num_queries} 回の単一ドキュメント検索ベンチマークを開始します ---")
    
    # 検索対象のドキュメントを事前に挿入
    inserted_docs = []
    for i in range(num_queries):
        doc = {
            "name": generate_random_string(15),
            "value": random.randint(1, 10000),
            "timestamp": time.time(),
            "unique_id": f"query_{i}" # ユニークなIDで検索
        }
        inserted_docs.append(db_instance.insert_one(TEST_COLLECTION, doc, TEST_OWNER_ID))
    
    # インデックスを作成 (もしあれば)
    db_instance.create_index(TEST_COLLECTION, "unique_id")

    start_time = time.perf_counter()
    for doc in inserted_docs:
        query = {"unique_id": doc["unique_id"]}
        found_doc = db_instance.find_one(TEST_COLLECTION, query, TEST_OWNER_ID)
        assert found_doc is not None and found_doc["unique_id"] == doc["unique_id"], "検索失敗"
    end_time = time.perf_counter()

    elapsed_time = end_time - start_time
    avg_time_per_query = elapsed_time / num_queries
    logger.info(f"検索完了。合計時間: {elapsed_time:.4f} 秒")
    logger.info(f"1 クエリあたりの平均検索時間: {avg_time_per_query:.6f} 秒")
    logger.info(f"スループット: {num_queries / elapsed_time:.2f} クエリ/秒")
    return elapsed_time

def benchmark_update_one(num_updates: int):
    """単一ドキュメント更新 (update_one) のベンチマーク"""
    logger.info(f"--- {num_updates} 回の単一ドキュメント更新ベンチマークを開始します ---")

    # 更新対象のドキュメントを事前に挿入
    inserted_docs = []
    for i in range(num_updates):
        doc = {
            "name": generate_random_string(15),
            "status": "pending",
            "update_id": f"update_{i}" # ユニークなIDで更新
        }
        inserted_docs.append(db_instance.insert_one(TEST_COLLECTION, doc, TEST_OWNER_ID))
    
    # インデックスを作成 (もしあれば)
    db_instance.create_index(TEST_COLLECTION, "update_id")

    start_time = time.perf_counter()
    for doc in inserted_docs:
        query = {"update_id": doc["update_id"]}
        update_data = {"status": "completed", "updated_at": time.time()}
        updated_doc = db_instance.update_one(TEST_COLLECTION, query, update_data, TEST_OWNER_ID)
        assert updated_doc is not None and updated_doc["status"] == "completed", "更新失敗"
    end_time = time.perf_counter()

    elapsed_time = end_time - start_time
    avg_time_per_update = elapsed_time / num_updates
    logger.info(f"更新完了。合計時間: {elapsed_time:.4f} 秒")
    logger.info(f"1 ドキュメントあたりの平均更新時間: {avg_time_per_update:.6f} 秒")
    logger.info(f"スループット: {num_updates / elapsed_time:.2f} 更新/秒")
    return elapsed_time

def benchmark_delete_one(num_deletes: int):
    """単一ドキュメント削除 (delete_one) のベンチマーク"""
    logger.info(f"--- {num_deletes} 回の単一ドキュメント削除ベンチマークを開始します ---")

    # 削除対象のドキュメントを事前に挿入
    inserted_docs = []
    for i in range(num_deletes):
        doc = {
            "name": generate_random_string(15),
            "delete_id": f"delete_{i}" # ユニークなIDで削除
        }
        inserted_docs.append(db_instance.insert_one(TEST_COLLECTION, doc, TEST_OWNER_ID))
    
    # インデックスを作成 (もしあれば)
    db_instance.create_index(TEST_COLLECTION, "delete_id")

    start_time = time.perf_counter()
    for doc in inserted_docs:
        query = {"delete_id": doc["delete_id"]}
        deleted_doc = db_instance.delete_one(TEST_COLLECTION, query, TEST_OWNER_ID)
        assert deleted_doc is not None and deleted_doc["delete_id"] == doc["delete_id"], "削除失敗"
    end_time = time.perf_counter()

    elapsed_time = end_time - start_time
    avg_time_per_delete = elapsed_time / num_deletes
    logger.info(f"削除完了。合計時間: {elapsed_time:.4f} 秒")
    logger.info(f"1 ドキュメントあたりの平均削除時間: {avg_time_per_delete:.6f} 秒")
    logger.info(f"スループット: {num_deletes / elapsed_time:.2f} 削除/秒")
    return elapsed_time

if __name__ == "__main__":
    logger.info("=====================================")
    logger.info("  AstroDB ベンチマークテスト開始")
    logger.info("=====================================")

    # ベンチマーク設定
    NUM_DOCUMENTS_INSERT = 1000
    NUM_QUERIES_FIND = 500
    NUM_UPDATES = 500
    NUM_DELETES = 500

    try:
        setup_benchmark_db()

        # 挿入ベンチマーク
        insert_time = benchmark_insert_one(NUM_DOCUMENTS_INSERT)
        
        # 検索ベンチマーク
        find_time = benchmark_find_one(NUM_QUERIES_FIND)

        # 更新ベンチマーク
        update_time = benchmark_update_one(NUM_UPDATES)

        # 削除ベンチマーク
        delete_time = benchmark_delete_one(NUM_DELETES)

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
        teardown_benchmark_db()
        logger.info("=====================================")
        logger.info("  AstroDB ベンチマークテスト終了")
        logger.info("=====================================")
