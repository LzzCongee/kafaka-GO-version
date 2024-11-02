package main

import (
	"fmt"
	"net"
	"os"
	"encoding/binary"
	"bytes"
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
	for {
		// read the request message
		request := make([]byte, 512) 
		_, err := conn.Read(request) // n表示实际读取的字节数 n stands for the number of actually read bytes
		if err != nil {
			fmt.Println("读取请求时出错：", err.Error())
			return
		}
		
		response := createResponse(request) 

		// send the response to the cliend
		if _, err := conn.Write(response); err != nil {
			fmt.Println("Error while writing: ", err.Error())
		}
	}
}

/*
	message length => 4 bytes
	correlation ID => 4 bytes
	error code => 2 bytes

	#apikeys +1 -> 2 => 1 byte
	apikey (18)		 => 2 bytes  0, 18
	MinVersion for ApiKey 18 (v0) => 2 bytes 0, 0
	MaxVersion for ApiKey 18 (v4) => 2 bytes 0, 4
	Tag Buffer(tagged_fields) 	=> 1 byte 0 
	
	throttle_time_ms			=> 4 bytes 0, 0, 0, 0
	Tag Buffer => 1 byte 0
*/
func createResponse( req []byte ) []byte {
	requestHeader := req[4:]
    response := make([]byte, 0) // 创建一个空的切片 response := bytes.NewBuffer([]byte{})

    response = append(response, []byte{0, 0, 0, 26}...) 	// mssgSize
    response = append(response, requestHeader[4:8]...)      // corrId
    var apiVersion int16
    err := binary.Read(bytes.NewReader(requestHeader[2:4]), binary.BigEndian, &apiVersion)
    if err != nil {
        return nil
    }
    if apiVersion < 0 || apiVersion > 4 {
        response = append(response, []byte{0, 35}...) // error code '35'
    } else {
        response = append(response, []byte{0, 0}...) // error code '0'
    }

    response = append(response, []byte{3}...)          // num of api keys + 1
    response = append(response, []byte{0, 18}...)      // apikey_18 APIVersions
    response = append(response, []byte{0, 0}...)       // min version 0
    response = append(response, []byte{0, 4}...)       // max version 4
	response = append(response, []byte{0}...)          // Tag Buffer

	response = append(response, []byte{0, 75}...)      // apikey_75 DescribeTopicPartitions
    response = append(response, []byte{0, 0}...)       // min version 0
    response = append(response, []byte{0, 0}...)       // max version 0
    response = append(response, []byte{0}...)          // Tag Buffer

    response = append(response, []byte{0, 0, 0, 0}...) // throttle_time_ms
    response = append(response, []byte{0}...)          // Tag Buffer
    return response

}
