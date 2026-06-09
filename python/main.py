from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
import json
from typing import Dict, Set, List
from collections import defaultdict
from datetime import datetime

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# Хранилище: room_name -> { "messages": list, "users": set }
rooms: Dict[str, Dict] = defaultdict(lambda: {"messages": [], "users": set()})

class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = defaultdict(set)
        self.user_room: Dict[WebSocket, str] = {}
        self.user_nickname: Dict[WebSocket, str] = {}

    async def connect(self, websocket: WebSocket, room: str, nickname: str):
        await websocket.accept()
        self.active_connections[room].add(websocket)
        self.user_room[websocket] = room
        self.user_nickname[websocket] = nickname
        # Добавить пользователя в хранилище комнаты
        rooms[room]["users"].add(nickname)
        # Отправить историю
        history = rooms[room]["messages"][-50:]
        await websocket.send_json({"type": "history", "messages": history})
        # Отправить список пользователей этому клиенту
        await self.send_user_list(room)
        # Уведомить остальных о новом пользователе
        await self.broadcast(room, {
            "type": "message",
            "nickname": "system",
            "text": f"{nickname} присоединился",
            "timestamp": int(datetime.now().timestamp() * 1000)
        }, exclude=websocket)

    async def disconnect(self, websocket: WebSocket):
        room = self.user_room.get(websocket)
        nickname = self.user_nickname.get(websocket)
        if room and websocket in self.active_connections[room]:
            self.active_connections[room].remove(websocket)
            rooms[room]["users"].discard(nickname)
            await self.send_user_list(room)
            await self.broadcast(room, {
                "type": "message",
                "nickname": "system",
                "text": f"{nickname} покинул комнату",
                "timestamp": int(datetime.now().timestamp() * 1000)
            })
        if websocket in self.user_room:
            del self.user_room[websocket]
        if websocket in self.user_nickname:
            del self.user_nickname[websocket]

    async def broadcast(self, room: str, message: dict, exclude: WebSocket = None):
        for connection in self.active_connections[room]:
            if connection != exclude:
                await connection.send_json(message)

    async def send_user_list(self, room: str):
        users = list(rooms[room]["users"])
        await self.broadcast(room, {"type": "user_list", "users": users})

    async def add_message(self, room: str, nickname: str, text: str):
        msg = {
            "type": "message",
            "nickname": nickname,
            "text": text,
            "timestamp": int(datetime.now().timestamp() * 1000)
        }
        rooms[room]["messages"].append(msg)
        # ограничим историю
        if len(rooms[room]["messages"]) > 50:
            rooms[room]["messages"].pop(0)
        await self.broadcast(room, msg)

manager = ConnectionManager()

@app.get("/")
async def get():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    # Ждём первое сообщение с join
    data = await websocket.receive_text()
    try:
        join_data = json.loads(data)
        if join_data.get("type") == "join":
            nickname = join_data["nickname"]
            room = join_data["room"]
            await manager.connect(websocket, room, nickname)
        else:
            await websocket.close()
            return
    except Exception:
        await websocket.close()
        return

    try:
        while True:
            raw = await websocket.receive_text()
            data = json.loads(raw)
            if data.get("type") == "message":
                room = manager.user_room.get(websocket)
                nickname = manager.user_nickname.get(websocket)
                if room and nickname:
                    await manager.add_message(room, nickname, data["text"])
    except WebSocketDisconnect:
        await manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
