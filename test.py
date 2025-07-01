import asyncio
import websockets
import hashlib


async def test_websocket_connection():
    uri = "ws://localhost:8000/"
    async with websockets.connect(uri) as websocket:
        # 1. CONNECT_REQ送信
        pepper = "my_pepper"
        await websocket.send(f"CLIENT_WAIT/CONNECT_REQ/PEPPER:{pepper}")
        # 2. サーバーからの応答受信
        response = await websocket.recv()
        print("Step1 Response:", response)
        assert response.startswith("SERVER/CRQ_SUCCESS"), "CRQ_SUCCESS expected"
        # 3. IDとSALT取得
        parts = response.split("/")
        id_part = [p for p in parts if p.startswith("YOUR_ID:")][0]
        id = id_part.split(":")[1]
        salt_part = [p for p in parts if p.startswith("SALT:")][0]
        salt = salt_part.split(":")[1]
        # 4. ハッシュ計算
        base_text = f"{id}:{pepper}:{salt}"
        hash = hashlib.sha256(base_text.encode()).hexdigest()
        # 5. CONNECT_SUCCESS送信
        await websocket.send(f"CLIENT_{id}/CONNECT_SUCCESS/HASH:{hash}")
        # 6. サーバーからの応答受信
        response2 = await websocket.recv()
        print("Step2 Response:", response2)
        assert response2 == "SERVER/CONNECT_SUCCESS", "CONNECT_SUCCESS expected"


if __name__ == "__main__":
    asyncio.run(test_websocket_connection())
