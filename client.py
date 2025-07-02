import asyncio
import websockets
import ujson
import getpass
import logging

# ロガーの設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERVER_URL = "ws://127.0.0.1:8000/ws"

class AstroDBClient:
    def __init__(self):
        self.token = None

    async def connect(self):
        """サーバーに接続し、対話ループを開始する"""
        try:
            async with websockets.connect(SERVER_URL) as websocket:
                logger.info("AstroDBサーバーに接続しました。コマンドを入力してください (例: help)")
                self.websocket = websocket
                await self.command_loop()
        except websockets.exceptions.ConnectionClosedError:
            logger.error("エラー: サーバーに接続できませんでした。サーバーが起動しているか確認してください。")
        except Exception as e:
            logger.error(f"予期せぬエラーが発生しました: {e}")

    async def send_command(self, command: dict):
        """コマンドをサーバーに送信し、応答を待つ"""
        if self.token and "token" not in command:
            command["token"] = self.token
        
        await self.websocket.send(ujson.dumps(command))
        response_raw = await self.websocket.recv()
        response = ujson.loads(response_raw)
        return response

    async def command_loop(self):
        """ユーザーからの入力を受け付け、コマンドを処理するループ"""
        while True:
            try:
                user_input = await asyncio.to_thread(input, "> ")
                parts = user_input.strip().split()
                if not parts:
                    continue
                
                cmd = parts[0].lower()

                if cmd == "exit":
                    logger.info("クライアントを終了します。")
                    break
                elif cmd == "help":
                    self.print_help()
                elif cmd == "register":
                    await self.handle_register()
                elif cmd == "login":
                    await self.handle_login()
                elif cmd == "insert":
                    await self.handle_insert(parts)
                elif cmd == "insert_many":
                    await self.handle_insert_many(parts)
                elif cmd == "find":
                    await self.handle_find(parts)
                elif cmd == "update":
                    await self.handle_update(parts)
                elif cmd == "update_many":
                    await self.handle_update_many(parts)
                elif cmd == "delete":
                    await self.handle_delete(parts)
                elif cmd == "delete_many":
                    await self.handle_delete_many(parts)
                elif cmd == "restore":
                    await self.handle_restore(parts)
                elif cmd == "change_password":
                    await self.handle_change_password()
                else:
                    logger.warning(f"不明なコマンドです: {cmd}")

            except (EOFError, KeyboardInterrupt):
                logger.info("\nクライアントを終了します。")
                break

    def print_help(self):
        logger.info("\n--- AstroDB Client Help ---")
        logger.info("register          - 新しいユーザーを登録します")
        logger.info("login             - サーバーにログインします")
        logger.info("insert <coll> <json> - ドキュメントを挿入します (例: insert posts {\"title\":\"hello\"})")
        logger.info("insert_many <coll> <json_array> - 複数のドキュメントを挿入します (例: insert_many posts [{\"title\":\"hello\"}, {\"title\":\"world\"}])")
        logger.info("find <coll> <json>   - ドキュメントを検索します (例: find posts {\"title\":\"hello\"})")
        logger.info("update <coll> <query> <json> - ドキュメントを更新します (例: update posts {\"title\":\"hello\"} {\"status\":\"published\"})")
        logger.info("update_many <coll> <query> <json> - 複数のドキュメントを更新します")
        logger.info("delete <coll> <query>  - ドキュメントを削除します (例: delete posts {\"title\":\"hello\"})")
        logger.info("delete_many <coll> <query> - 複数のドキュメントを削除します")
        logger.info("find_one <coll> <json> - ドキュメントを1つ検索します (例: find_one posts {\"title\":\"hello\"})")
        logger.info("find_many <coll> <json> - ドキュメントを複数検索します (例: find_many posts {\"status\":\"published\"})")
        logger.info("backup            - データベースをバックアップします")
        logger.info("restore <filename> - 指定されたバックアップファイルからデータベースを復元します")
        logger.info("help              - このヘルプメッセージを表示します")
        logger.info("exit              - クライアントを終了します")
        logger.info("---------------------------\n")

    async def handle_register(self):
        username = await asyncio.to_thread(input, "  Username: ")
        password = await asyncio.to_thread(getpass.getpass, "  Password: ")
        command = {"command": "REGISTER", "username": username, "password": password}
        response = await self.send_command(command)
        if response.get("status") == "error":
            error_code = response.get("code")
            message = response.get("message", "不明なエラーが発生しました。")
            if error_code == "USER_ALREADY_EXISTS":
                logger.error(f"< エラー: ユーザー '{username}' は既に存在します。")
            else:
                logger.error(f"< エラー ({error_code}): {message}")
        else:
            logger.info(f"< {response}")

    async def handle_login(self):
        username = await asyncio.to_thread(input, "  Username: ")
        password = await asyncio.to_thread(getpass.getpass, "  Password: ")
        command = {"command": "LOGIN", "username": username, "password": password}
        response = await self.send_command(command)
        if response.get("status") == "ok" and response.get("token"):
            self.token = response["token"]
            logger.info("< ログイン成功。認証トークンを保存しました。")
        else:
            error_code = response.get("code")
            message = response.get("message", "不明なエラーが発生しました。")
            if error_code == "INVALID_CREDENTIALS":
                logger.error("< エラー: 無効なユーザー名またはパスワードです。")
            else:
                logger.error(f"< エラー ({error_code}): {message}")

    async def handle_insert(self, parts):
        if not self.token:
            logger.error("< エラー: このコマンドを実行するにはまずログインしてください。")
            return

        if len(parts) < 3:
            logger.warning("< 使用法: insert <collection_name> <json_document>")
            return
        
        collection = parts[1]
        doc_str = " ".join(parts[2:])
        try:
            document = ujson.loads(doc_str)
            command = {"command": "INSERT_ONE", "collection": collection, "document": document}
            response = await self.send_command(command)
            logger.info(f"< {response}")
        except ValueError:
            logger.error("< エラー: 無効なJSON形式です。キーと文字列はダブルクォートで囲ってください。")

    async def handle_insert_many(self, parts):
        if not self.token:
            logger.error("< エラー: このコマンドを実行するにはまずログインしてください。")
            return

        if len(parts) < 3:
            logger.warning("< 使用法: insert_many <collection_name> <json_array_of_documents>")
            return
        
        collection = parts[1]
        docs_str = " ".join(parts[2:])
        try:
            documents = ujson.loads(docs_str)
            if not isinstance(documents, list):
                logger.error("< エラー: insert_many にはJSON配列を指定してください。")
                return
            command = {"command": "INSERT_MANY", "collection": collection, "documents": documents}
            response = await self.send_command(command)
            logger.info(f"< {response}")
        except ValueError:
            logger.error("< エラー: 無効なJSON形式です。キーと文字列はダブルクォートで囲ってください。")

    async def handle_find(self, parts):
        if not self.token:
            logger.error("< エラー: このコマンドを実行するにはまずログインしてください。")
            return

        if len(parts) < 2:
            logger.warning("< 使用法: find <collection_name> [json_query]")
            return
        
        collection = parts[1]
        query_str = " ".join(parts[2:]) if len(parts) > 2 else "{}"
        try:
            query = ujson.loads(query_str)
            command = {"command": "FIND", "collection": collection, "query": query}
            response = await self.send_command(command)
            logger.info(f"< {response}")
        except ValueError:
            logger.error("< エラー: 無効なJSON形式です。キーと文字列はダブルクォートで囲ってください。")

    async def handle_update(self, parts):
        if not self.token:
            logger.error("< エラー: このコマンドを実行するにはまずログインしてください。")
            return

        if len(parts) < 4:
            logger.warning("< 使用法: update <collection_name> <json_query> <json_update_data>")
            return
        
        collection = parts[1]
        # ここで、クエリと更新データを正しく分割する必要があります。
        # 簡単な実装として、最初の '{' から '}' までをクエリと仮定します。
        try:
            user_input_str = " ".join(parts[2:])
            first_brace = user_input_str.find('{')
            last_brace = user_input_str.rfind('}')
            # この方法はネストされたJSONに弱いですが、基本的なケースに対応します
            # より堅牢な実装には正規表現などが必要です
            query_str = user_input_str[:last_brace + 1]
            update_str = user_input_str[last_brace + 2:]

            query = ujson.loads(query_str)
            update_data = ujson.loads(update_str)
            command = {"command": "UPDATE_ONE", "collection": collection, "query": query, "update_data": update_data}
            response = await self.send_command(command)
            logger.info(f"< {response}")
        except (ValueError, IndexError):
            logger.error("< エラー: 無効なJSON形式またはコマンド形式です。キーと文字列はダブルクォートで囲ってください。")

    async def handle_update_many(self, parts):
        if not self.token:
            logger.error("< エラー: このコマンドを実行するにはまずログインしてください。")
            return

        if len(parts) < 4:
            logger.warning("< 使用法: update_many <collection_name> <json_query> <json_update_data>")
            return
        
        collection = parts[1]
        try:
            user_input_str = " ".join(parts[2:])
            first_brace = user_input_str.find('{')
            last_brace = user_input_str.rfind('}')
            query_str = user_input_str[:last_brace + 1]
            update_str = user_input_str[last_brace + 2:]

            query = ujson.loads(query_str)
            update_data = ujson.loads(update_str)
            command = {"command": "UPDATE_MANY", "collection": collection, "query": query, "update_data": update_data}
            response = await self.send_command(command)
            logger.info(f"< {response}")
        except (ValueError, IndexError):
            logger.error("< エラー: 無効なJSON形式またはコマンド形式です。キーと文字列はダブルクォートで囲ってください。")

    async def handle_delete(self, parts):
        if not self.token:
            logger.error("< エラー: このコマンドを実行するにはまずログインしてください。")
            return

        if len(parts) < 2:
            logger.warning("< 使用法: delete <collection_name> [json_query]")
            return
        
        collection = parts[1]
        query_str = " ".join(parts[2:]) if len(parts) > 2 else "{}"
        try:
            query = ujson.loads(query_str)
            command = {"command": "DELETE_ONE", "collection": collection, "query": query}
            response = await self.send_command(command)
            logger.info(f"< {response}")
        except ValueError:
            logger.error("< エラー: 無効なJSON形式です。キーと文字列はダブルクォートで囲ってください。")

    async def handle_delete_many(self, parts):
        if not self.token:
            logger.error("< エラー: このコマンドを実行するにはまずログインしてください。")
            return

        if len(parts) < 2:
            logger.warning("< 使用法: delete_many <collection_name> [json_query]")
            return
        
        collection = parts[1]
        query_str = " ".join(parts[2:]) if len(parts) > 2 else "{}"
        try:
            query = ujson.loads(query_str)
            command = {"command": "DELETE_MANY", "collection": collection, "query": query}
            response = await self.send_command(command)
            logger.info(f"< {response}")
        except ValueError:
            logger.error("< エラー: 無効なJSON形式です。キーと文字列はダブルクォートで囲ってください。")

if __name__ == "__main__":
    client = AstroDBClient()
    try:
        asyncio.run(client.connect())
    except KeyboardInterrupt:
        logger.info("\nクライアントが中断されました。")