import asyncio
import websockets
import ujson
import getpass

SERVER_URL = "ws://127.0.0.1:8000/ws"

class AstroDBClient:
    def __init__(self):
        self.token = None

    async def connect(self):
        """サーバーに接続し、対話ループを開始する"""
        try:
            async with websockets.connect(SERVER_URL) as websocket:
                print("AstroDBサーバーに接続しました。コマンドを入力してください (例: help)")
                self.websocket = websocket
                await self.command_loop()
        except websockets.exceptions.ConnectionClosedError:
            print("エラー: サーバーに接続できませんでした。サーバーが起動しているか確認してください。")
        except Exception as e:
            print(f"予期せぬエラーが発生しました: {e}")

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
                    print("クライアントを終了します。")
                    break
                elif cmd == "help":
                    self.print_help()
                elif cmd == "register":
                    await self.handle_register()
                elif cmd == "login":
                    await self.handle_login()
                elif cmd == "insert":
                    await self.handle_insert(parts)
                elif cmd == "find":
                    await self.handle_find(parts)
                elif cmd == "update":
                    await self.handle_update(parts)
                elif cmd == "delete":
                    await self.handle_delete(parts)
                elif cmd == "find_one":
                    await self.handle_find_one(parts)
                else:
                    print(f"不明なコマンドです: {cmd}")

            except (EOFError, KeyboardInterrupt):
                print("\nクライアントを終了します。")
                break

    def print_help(self):
        print("\n--- AstroDB Client Help ---")
        print("register          - 新しいユーザーを登録します")
        print("login             - サーバーにログインします")
        print("insert <coll> <json> - ドキュメントを挿入します (例: insert posts {\"title\":\"hello\"})")
        print("find <coll> <json>   - ドキュメントを検索します (例: find posts {\"title\":\"hello\"})")
        print("update <coll> <query> <json> - ドキュメントを更新します (例: update posts {\"title\":\"hello\"} {\"status\":\"published\"})")
        print("delete <coll> <query>  - ドキュメントを削除します (例: delete posts {\"title\":\"hello\"})")
        print("find_one <coll> <json> - ドキュメントを1つ検索します (例: find_one posts {\"title\":\"hello\"})")
        print("help              - このヘルプメッセージを表示します")
        print("exit              - クライアントを終了します")
        print("---------------------------\n")

    async def handle_register(self):
        username = await asyncio.to_thread(input, "  Username: ")
        password = await asyncio.to_thread(getpass.getpass, "  Password: ")
        command = {"command": "REGISTER", "username": username, "password": password}
        response = await self.send_command(command)
        print(f"< {response}")

    async def handle_login(self):
        username = await asyncio.to_thread(input, "  Username: ")
        password = await asyncio.to_thread(getpass.getpass, "  Password: ")
        command = {"command": "LOGIN", "username": username, "password": password}
        response = await self.send_command(command)
        if response.get("status") == "ok" and response.get("token"):
            self.token = response["token"]
            print("< ログイン成功。認証トークンを保存しました。")
        else:
            print(f"< {response}")

    async def handle_insert(self, parts):
        if not self.token:
            print("< エラー: このコマンドを実行するにはまずログインしてください。")
            return

        if len(parts) < 3:
            print("< 使用法: insert <collection_name> <json_document>")
            return
        
        collection = parts[1]
        doc_str = " ".join(parts[2:])
        try:
            document = ujson.loads(doc_str)
            command = {"command": "INSERT_ONE", "collection": collection, "document": document}
            response = await self.send_command(command)
            print(f"< {response}")
        except ValueError:
            print("< エラー: 無効なJSON形式です。キーと文字列はダブルクォートで囲ってください。")

if __name__ == "__main__":
    client = AstroDBClient()
    try:
        asyncio.run(client.connect())
    except KeyboardInterrupt:
        print("\nクライアントが中断されました。")