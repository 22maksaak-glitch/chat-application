package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Message struct {
	Type      string `json:"type"`
	Nickname  string `json:"nickname,omitempty"`
	Text      string `json:"text,omitempty"`
	Timestamp int64  `json:"timestamp,omitempty"`
	Users     []string `json:"users,omitempty"`
	Messages  []Message `json:"messages,omitempty"`
}

type Room struct {
	sync.RWMutex
	messages []Message
	users    map[*Client]string // client -> nickname
}

type Client struct {
	conn     *websocket.Conn
	send     chan Message
	room     *Room
	nickname string
}

var (
	rooms = make(map[string]*Room)
	mu    sync.RWMutex
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func getOrCreateRoom(roomName string) *Room {
	mu.Lock()
	defer mu.Unlock()
	if room, ok := rooms[roomName]; ok {
		return room
	}
	room := &Room{
		messages: []Message{},
		users:    make(map[*Client]string),
	}
	rooms[roomName] = room
	return room
}

func (r *Room) addMessage(msg Message) {
	r.Lock()
	defer r.Unlock()
	r.messages = append(r.messages, msg)
	if len(r.messages) > 50 {
		r.messages = r.messages[1:]
	}
}

func (r *Room) broadcast(msg Message, exclude *Client) {
	r.RLock()
	defer r.RUnlock()
	for client := range r.users {
		if client != exclude {
			select {
			case client.send <- msg:
			default:
				close(client.send)
				delete(r.users, client)
			}
		}
	}
}

func (r *Room) getUserList() []string {
	r.RLock()
	defer r.RUnlock()
	names := make([]string, 0, len(r.users))
	for _, nick := range r.users {
		names = append(names, nick)
	}
	return names
}

func (c *Client) readPump() {
	defer func() {
		c.room.Lock()
		delete(c.room.users, c)
		c.room.Unlock()
		c.conn.Close()
		userList := c.room.getUserList()
		c.room.broadcast(Message{Type: "user_list", Users: userList}, nil)
		c.room.broadcast(Message{
			Type:      "message",
			Nickname:  "system",
			Text:      c.nickname + " покинул комнату",
			Timestamp: time.Now().UnixMilli(),
		}, nil)
	}()

	for {
		var msg Message
		err := c.conn.ReadJSON(&msg)
		if err != nil {
			break
		}
		if msg.Type == "message" {
			fullMsg := Message{
				Type:      "message",
				Nickname:  c.nickname,
				Text:      msg.Text,
				Timestamp: time.Now().UnixMilli(),
			}
			c.room.addMessage(fullMsg)
			c.room.broadcast(fullMsg, nil)
		}
	}
}

func (c *Client) writePump() {
	for msg := range c.send {
		err := c.conn.WriteJSON(msg)
		if err != nil {
			break
		}
	}
}

func serveWs(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print(err)
		return
	}

	var initMsg Message
	err = conn.ReadJSON(&initMsg)
	if err != nil || initMsg.Type != "join" {
		conn.Close()
		return
	}
	room := getOrCreateRoom(initMsg.Text) // в initMsg.Text храним room, nickname?
	// В реализации клиент отправляет: {"type":"join","nickname":"Alex","room":"general"}
	// Но из-за ограничений JSON, переделаем: initMsg.Nickname, initMsg.Text?
	// Упростим: ожидаем {type:"join", nickname:"Alex", room:"general"}
	// Перечитаем:
}

// Более корректная обработка join:
type JoinMsg struct {
	Type     string `json:"type"`
	Nickname string `json:"nickname"`
	Room     string `json:"room"`
}

func serveWsCorrect(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print(err)
		return
	}
	var join JoinMsg
	err = conn.ReadJSON(&join)
	if err != nil || join.Type != "join" {
		conn.Close()
		return
	}
	room := getOrCreateRoom(join.Room)
	client := &Client{
		conn:     conn,
		send:     make(chan Message, 256),
		room:     room,
		nickname: join.Nickname,
	}
	room.Lock()
	room.users[client] = join.Nickname
	room.Unlock()

	// История
	room.RLock()
	history := make([]Message, len(room.messages))
	copy(history, room.messages)
	room.RUnlock()
	client.send <- Message{Type: "history", Messages: history}
	// Список пользователей
	userList := room.getUserList()
	client.send <- Message{Type: "user_list", Users: userList}
	// Оповестить остальных
	room.broadcast(Message{
		Type:      "message",
		Nickname:  "system",
		Text:      join.Nickname + " присоединился",
		Timestamp: time.Now().UnixMilli(),
	}, client)

	go client.writePump()
	client.readPump()
}

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "static/index.html")
	})
	http.HandleFunc("/ws", serveWsCorrect)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
