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
	// defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error while accepting the connection: ", err.Error())
			os.Exit(1)
		}
		
		go handleConnection(conn)
	}

}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// read the request message
	request := make([]byte, 512) 
	n, err := conn.Read(request) // n表示实际读取的字节数 n stands for the number of actually read bytes
	if err != nil {
		fmt.Println("读取请求时出错：", err.Error())
		return
	}
	// fmt.Printf("十六进制格式: %x\n", request[8:12])

	// 准备响应 response
	messageSize := binary.BigEndian.Uint32( request[0:4] ) // request message size
	if messageSize < 4 {
		fmt.Println("请求消息大小不正确")
		return
	}
	// Check if the received data is complete
	if n < int(messageSize) {
		fmt.Println("接收到的数据不完整")
		return
	}

	request_api_key := binary.BigEndian.Uint16( request[4:6] ) 		// api_key : api version
	request_api_version := binary.BigEndian.Uint16( request[6:8] )	// api_version 
	correlationID := binary.BigEndian.Uint32(request[8:12]) 		// extract correlation id
	// error codes
	error_code := 0
	if request_api_version != 0 && request_api_version != 1 && request_api_version != 2 && request_api_version != 3 && request_api_version != 4 {
		error_code = 35
	}

	// filling response slice
	response := make([]byte, 23) 

	binary.BigEndian.PutUint32(response[0:4], 19 ) 	// response message size
	binary.BigEndian.PutUint32(response[4:8], uint32(correlationID))
	binary.BigEndian.PutUint16(response[8:10], uint16(error_code)) 
	/*
	   Breakdown:
	       - First Byte: #apikeys +1 -> 2
	       - Next two bytes: API key; supposed to be 18 according to spec -> 0, 18
	       - Next two bytes: min version; supposed to be 0 -> 0, 0
	       - Next two bytes: max version; supposed to be 4 -> 0, 4
	       - Next byte: TAG_BUFFER -> 0
	       - Next four bytes: throttle_time_ms -> 0, 0, 0, 0
	       - Final byte: TAG_BUFFER -> 0
	*/
	response[10] = 2                              								// Number of API keys
	binary.BigEndian.PutUint16(response[11:13], uint16(request_api_key))  		// API Key 1 - API_VERSIONS
	binary.BigEndian.PutUint16(response[13:15], 0 )  							// min_version
	binary.BigEndian.PutUint16(response[15:17], 4 ) 							// max_version
	response[17] = 0  // tagged_fields
	binary.BigEndian.PutUint32(response[18:22], 0 ) // throttle_time_ms
	response[22] = 0	// TAG_BUFFER
	 

	// send the response to the cliend
	if _, err := conn.Write(response); err != nil {
		fmt.Println("Error while writing: ", err.Error())
	}
}


