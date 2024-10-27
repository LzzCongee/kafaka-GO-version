package main

import (
	"fmt"
	"net"
	"os"
	
	"encoding/binary"
)

var _ = net.Listen
var _ = os.Exit

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("接受连接时出错：", err.Error())
			os.Exit(1)
		}
		
		go handleConnection(conn)
	}

}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// 准备响应
	correlationID := int32(7) // 硬编码的关联 ID
	messageSize := int32(4)    // 消息大小（仅头部，没有主体）

	// 创建响应缓冲区
	response := make([]byte, 8) // 4 字节消息大小 + 4 字节关联 ID

	// 以大端顺序写入消息大小
	binary.BigEndian.PutUint32(response[0:4], uint32(messageSize))
	// 以大端顺序写入关联 ID
	binary.BigEndian.PutUint32(response[4:8], uint32(correlationID))

	// 将响应发送回客户端
	if _, err := conn.Write(response); err != nil {
		fmt.Println("写入响应时出错：", err.Error())
	}
}