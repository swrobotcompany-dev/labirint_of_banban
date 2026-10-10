#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Сервер для многопользовательской игры "Сад Монстров 3D".

Запуск:  python3 server.py [порт]   (по умолчанию 8765)

Что делает:
  1. Поднимает обычный HTTP-сервер на http://localhost:<порт+1> (по умолчанию 8766),
     где лежит index.html — игроки открывают его в браузере.
  2. Поднимает WebSocket-сервер на ws://localhost:<порт> (без зависимостей,
     реализация RFC6455 встроена в этот файл).

Архитектура сети (host-and-authority упрощённая):
  - Первый подключившийся игрок становится ХОСТом: он генерирует лабиринт
    (seed уровня) и считает ИИ монстров и Банбана.
  - Остальные игроки шлют своё положение/поворот; хост рассылает всем
    состояние всех игроков + события (ключи, смерть, победа, смена уровня).
  - Если хост отваливается, сервер выбирает нового хоста из оставшихся
    и сообщает об этом всем клиентам.
"""

import asyncio
import base64
import hashlib
import json
import os
import socket
import struct
import threading
import time
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler

PORT = int(os.sys.argv[1]) if len(os.sys.argv) > 1 else 8765
HTTP_PORT = PORT + 1
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
ROOT = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
#  Минимальный WebSocket (RFC 6455), только текст, без внешних зависимостей
# ---------------------------------------------------------------------------
class WSConnection:
    def __init__(self, sock, peer):
        self.sock = sock
        self.peer = peer
        self.id = None
        self.alive = True
        self._send_lock = threading.Lock()
        self._recv_buf = b""

    # --- handshake -----------------------------------------------------
    @staticmethod
    def try_handshake(sock, peer):
        data = b""
        sock.settimeout(10)
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                return None
            data += chunk
            if len(data) > 65536:
                return None
        text = data.decode("latin-1")
        key = None
        for line in text.split("\r\n"):
            if line.lower().startswith("sec-websocket-key:"):
                key = line.split(":", 1)[1].strip()
        if not key:
            return None
        accept = base64.b64encode(
            hashlib.sha1((key + WS_GUID).encode()).digest()
        ).decode()
        resp = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Accept: %s\r\n\r\n" % accept
        )
        sock.sendall(resp.encode())
        sock.settimeout(None)
        return WSConnection(sock, peer)

    # --- frames --------------------------------------------------------
    def send_text(self, payload):
        data = payload.encode("utf-8")
        header = bytearray([0x81])  # FIN + text
        n = len(data)
        if n < 126:
            header.append(n)
        elif n < 65536:
            header.append(126)
            header += struct.pack("!H", n)
        else:
            header.append(127)
            header += struct.pack("!Q", n)
        with self._send_lock:
            if not self.alive:
                return False
            try:
                self.sock.sendall(bytes(header) + data)
                return True
            except OSError:
                self.alive = False
                return False

    def _read_exact(self, n):
        while len(self._recv_buf) < n:
            try:
                chunk = self.sock.recv(65536)
            except OSError:
                raise ConnectionError("socket closed")
            if not chunk:
                raise ConnectionError("socket closed")
            self._recv_buf += chunk
        out, self._recv_buf = self._recv_buf[:n], self._recv_buf[n:]
        return out

    def recv_text(self):
        """Возвращает строку или None, если соединение закрыто."""
        while True:
            b0, b1 = self._read_exact(2)
            opcode = b0 & 0x0F
            masked = b1 & 0x80
            length = b1 & 0x7F
            if length == 126:
                length = struct.unpack("!H", self._read_exact(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self._read_exact(8))[0]
            mask = self._read_exact(4) if masked else None
            payload = self._read_exact(length) if length else b""
            if mask:
                payload = bytes(p ^ mask[i % 4] for i, p in enumerate(payload))
            if opcode == 0x8:  # close
                return None
            if opcode == 0x9:  # ping -> pong
                self._send_raw(0xA, payload)
                continue
            if opcode in (0x1, 0x2):
                return payload.decode("utf-8", "replace")

    def _send_raw(self, opcode, payload):
        header = bytearray([0x80 | opcode, len(payload)])
        with self._send_lock:
            try:
                self.sock.sendall(bytes(header) + payload)
            except OSError:
                self.alive = False

    def close(self):
        self.alive = False
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


# ---------------------------------------------------------------------------
#  Игровая комната: маршрутизация сообщений между игроками
# ---------------------------------------------------------------------------
class Room:
    MAX_PLAYERS = 8

    def __init__(self):
        self.lock = threading.Lock()
        self.players = {}       # id -> {ws, name, last_seen}
        self.host_id = None
        self.next_id = 1
        self.state = {}         # последняя снапшот-инфа от хоста (для новых игроков)

    def join(self, ws, name):
        with self.lock:
            if len(self.players) >= self.MAX_PLAYERS:
                return None, "Комната полная (максимум %d игроков)." % self.MAX_PLAYERS
            pid = self.next_id
            self.next_id += 1
            self.players[pid] = {"ws": ws, "name": name, "last_seen": time.time()}
            ws.id = pid
            if self.host_id is None:
                self.host_id = pid
            roster = self._roster()
            return pid, roster

    def leave(self, pid):
        with self.lock:
            player = self.players.pop(pid, None)
            if not player:
                return None
            host_changed = False
            if self.host_id == pid:
                if self.players:
                    self.host_id = next(iter(sorted(self.players.keys())))
                    host_changed = True
                else:
                    self.host_id = None
                    self.state = {}
            return self._roster(), host_changed

    def touch(self, pid):
        with self.lock:
            p = self.players.get(pid)
            if p:
                p["last_seen"] = time.time()

    def broadcast(self, obj, exclude=None):
        data = json.dumps(obj, ensure_ascii=False)
        with self.lock:
            targets = [(i, p["ws"]) for i, p in self.players.items() if i != exclude]
        for _, ws in targets:
            ws.send_text(data)

    def send_to(self, pid, obj):
        with self.lock:
            p = self.players.get(pid)
        if p:
            p["ws"].send_text(json.dumps(obj, ensure_ascii=False))

    def _roster(self):
        return {
            "players": [{"id": i, "name": p["name"]} for i, p in self.players.items()],
            "hostId": self.host_id,
        }


ROOM = Room()


def handle_client(ws):
    try:
        first = ws.recv_text()
        if first is None:
            return
        msg = json.loads(first)
        if msg.get("type") != "join":
            return
        name = str(msg.get("name", ""))[:24] or "Игрок"
        pid, result = ROOM.join(ws, name)
        if pid is None:
            ws.send_text(json.dumps({"type": "full", "reason": result}, ensure_ascii=False))
            return
        # первому клиенту сообщаем начальный снапшот (обычно пустой)
        welcome = {"type": "welcome", "id": pid, "state": ROOM.state}
        welcome.update(result)
        ws.send_text(json.dumps(welcome, ensure_ascii=False))
        ROOM.broadcast({"type": "roster", **result}, exclude=pid)
        print("[+] %s (id=%d) зашёл. В комнате: %d" % (name, pid, len(ROOM.players)))

        while True:
            raw = ws.recv_text()
            if raw is None:
                break
            try:
                m = json.loads(raw)
            except ValueError:
                continue
            t = m.get("type")
            ROOM.touch(pid)
            if t == "ping":
                ws.send_text('{"type":"pong"}')
            elif t == "state":
                # сообщения от хоста: сохраняем и рассылаем остальным
                with ROOM.lock:
                    is_host = ROOM.host_id == pid
                    if is_host:
                        ROOM.state = m.get("data", {})
                if is_host:
                    ROOM.broadcast({"type": "state", "data": m.get("data", {})}, exclude=pid)
            elif t == "pos":
                # позиция не-хоста -> хосту
                host = ROOM.host_id
                if host is not None and host != pid:
                    m["id"] = pid
                    m["name"] = name
                    ROOM.send_to(host, m)
            elif t == "event":
                # игровые события от любого клиента -> всем (хост обработает у себя)
                m["id"] = pid
                ROOM.broadcast(m, exclude=pid)
    except ConnectionError:
        pass
    except Exception as e:
        print("[!] ошибка клиента: %r" % e)
    finally:
        res = ROOM.leave(ws.id)
        ws.close()
        if res:
            roster, host_changed = res
            ROOM.broadcast({"type": "roster", **roster, "hostChanged": host_changed})
            print("[-] id=%s вышел. В комнате: %d" % (ws.id, len(ROOM.players)))


# ---------------------------------------------------------------------------
#  HTTP-раздатчик index.html
# ---------------------------------------------------------------------------
class QuietHandler(SimpleHTTPRequestHandler):
    def __init__(self, *a, **kw):
        super().__init__(*a, directory=ROOT, **kw)

    def log_message(self, fmt, *args):
        pass


def http_thread():
    srv = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), QuietHandler)
    print("[HTTP]  http://localhost:%d/" % HTTP_PORT)
    srv.serve_forever()


def main():
    print("=" * 60)
    print("  Сад Монстров 3D — локальный сервер (%d игроков макс.)" % ROOM.MAX_PLAYERS)
    print("=" * 60)
    threading.Thread(target=http_thread, daemon=True).start()

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("0.0.0.0", PORT))
    listener.listen(16)
    print("[WS]    ws://localhost:%d/" % PORT)
    print("Откройте в браузере: http://localhost:%d  (на каждом компьютере)")
    print("Ctrl+C — остановить сервер.")

    try:
        while True:
            sock, peer = listener.accept()
            ws = WSConnection.try_handshake(sock, peer)
            if ws is None:
                sock.close()
                continue
            print("[*] соединение с %s:%d" % peer)
            threading.Thread(target=handle_client, args=(ws,), daemon=True).start()
    except KeyboardInterrupt:
        print("\nСервер остановлен.")


if __name__ == "__main__":
    main()
