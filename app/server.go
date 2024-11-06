package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// 类型别名
type API_KEY = int16
type API_ERROR_CODE = int16
type TOPIC_OPERATIONS = int32

// 定义常量
const (
	UNSUPPORTED_VERSION_ERROR_CODE API_ERROR_CODE = 35
	UNKNOWN_TOPIC_ERROR_CODE       API_ERROR_CODE = 3
)
const (
	API_VERSIONS              API_KEY = 18
	DESCRIBE_TOPIC_PARTITIONS API_KEY = 75
)
const (
	READ TOPIC_OPERATIONS = 0x00000df8
)

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

func createResponse(req []byte) []byte {
	basic_request_header := parseBasicRequestHeader(req)
	api_key := basic_request_header.api_key
	api_version := basic_request_header.api_version
	error_code := 0
	if api_version < 0 || api_version > 4 {
		error_code = 35
	}
	var final_response []byte
	// construct response according to the api_key
	if api_key == API_VERSIONS {
		fmt.Println("the api_key is ", API_VERSIONS)
		// create a api_version_response
		api_version_response := newAPIVersionsResponse(
			uint32(basic_request_header.correlation_id),
			int16(error_code),
			newAPIKeys( //APIVersionsRequest
				18,
				4,
				4,
			),
			newAPIKeys( //DescribeTopicPartition
				75,
				0,
				0,
			),
		)
		final_response = api_version_response.Encode()
		fmt.Printf("final_response:\n%x\n", final_response)
		setMessageSize(final_response)
		fmt.Printf("response_len:%d", final_response[0:4])

	} else if api_key == DESCRIBE_TOPIC_PARTITIONS {
		fmt.Println("the api_key is ", DESCRIBE_TOPIC_PARTITIONS)
		var req_struct RequestDescribeTopicPartitions
		req_struct.parseTopicRequest(req) // parse request
		response_body, _ := HandleRequestDescribeTopicPartitions(req_struct)

		final_response, _ = response_body.Serialize()
		fmt.Println(" final_response Serialized! ")
		fmt.Printf("final_response len: %d\n", len(final_response))

		setMessageSize(final_response)
		fmt.Printf("final_response:\n%x\n", final_response)
		fmt.Printf("response_rest_len:%d\n", final_response[0:4])

	} else {
		return []byte{}
	}
	return final_response
}

// Request: DescribeTopicPartitions  - struct and parser
type RequestDescribeTopicPartitions struct {
	correlation_id uint32
	client_id_len  uint16
	client_id      []byte
	// tag_buf_1           uint8
	topics_len          uint8
	topics              []TopicReq
	res_partition_limit uint32 // limits the number of partitions to be returned in the response.
	cursor              uint8  // A nullable field that can be used for pagination
	// tag_buf_2           uint8
}
type TopicReq struct {
	topic_name_len uint8
	topic_name     string
	// tag_buf        uint8
}

func (req_struct *RequestDescribeTopicPartitions) parseTopicRequest(req []byte) error {
	// parsing from pos 8
	req_struct.correlation_id = binary.BigEndian.Uint32(req[8:12])
	req_struct.client_id_len = binary.BigEndian.Uint16(req[12:14])
	cursor := 14
	req_struct.client_id = req[uint(cursor) : uint(cursor)+uint(req_struct.client_id_len)] // 该语句可能出现问题 切片索引溢出
	cursor += int(req_struct.client_id_len) + 1                                            // skip tag_buf
	req_struct.topics_len = uint8(req[cursor])                                             // array len
	cursor += 1

	for i := 0; i < int(req_struct.topics_len)-1; i++ {
		var new_topic TopicReq
		new_topic.topic_name_len = uint8(req[cursor]) // len(topic_name) + 1
		cursor += 1
		new_topic.topic_name = string(req[cursor : cursor+int(new_topic.topic_name_len-1)])
		cursor += int(new_topic.topic_name_len-1) + 1 // skip tag_buf
		req_struct.topics = append(req_struct.topics, new_topic)
	}
	req_struct.res_partition_limit = binary.BigEndian.Uint32(req[cursor : cursor+4])
	return nil
}

// response : DescribeTopicPartitions
type ResponseDescribeTopicPartitions struct {
	msg_size         uint32
	correlation_id   uint32
	tag_buf_1        uint8
	throttle_time_ms uint32
	topics_len       uint8
	topics_array     []TopicInfo
	cursor           uint8 //  0xff, indicating a null value.
	tag_buf_2        uint8
}
type TopicInfo struct {
	error_code        int16
	topic_name_len    uint8
	topic_name        string
	topic_id          []byte // 16bytes
	is_internal       uint8
	compact_array_len byte
	compact_array     int32 // authorized operations
	tag_buf           uint8
}

func HandleRequestDescribeTopicPartitions(req RequestDescribeTopicPartitions) (*ResponseDescribeTopicPartitions, error) {
	rspBody := &ResponseDescribeTopicPartitions{
		correlation_id:   req.correlation_id,
		throttle_time_ms: 0,
		topics_len:       req.topics_len,
		topics_array:     []TopicInfo{},
	}

	for idx := range req.topics {
		rspBody.topics_array = append(rspBody.topics_array, TopicInfo{
			error_code:        UNKNOWN_TOPIC_ERROR_CODE,
			topic_name_len:    req.topics[idx].topic_name_len,
			topic_name:        req.topics[idx].topic_name,
			topic_id:          []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, // 16 Byte
			is_internal:       0,
			compact_array_len: 0x01,
			compact_array:     READ,
		})
	}
	rspBody.cursor = 0xff
	return rspBody, nil
}

func (res_body *ResponseDescribeTopicPartitions) Serialize() ([]byte, error) {
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, res_body.msg_size)
	binary.Write(buff, binary.BigEndian, res_body.correlation_id)
	binary.Write(buff, binary.BigEndian, res_body.tag_buf_1)
	binary.Write(buff, binary.BigEndian, res_body.throttle_time_ms)
	binary.Write(buff, binary.BigEndian, res_body.topics_len)
	for _, topic := range res_body.topics_array {
		binary.Write(buff, binary.BigEndian, topic.error_code)
		binary.Write(buff, binary.BigEndian, topic.topic_name_len)
		buff.Write([]byte(topic.topic_name))

		buff.Write(topic.topic_id)
		binary.Write(buff, binary.BigEndian, topic.is_internal)
		binary.Write(buff, binary.BigEndian, topic.compact_array_len)
		binary.Write(buff, binary.BigEndian, topic.compact_array)
		binary.Write(buff, binary.BigEndian, topic.tag_buf)
	}
	binary.Write(buff, binary.BigEndian, res_body.cursor)
	binary.Write(buff, binary.BigEndian, res_body.tag_buf_2)

	return buff.Bytes(), nil
}

// Basic request header : struct and parser
type BasicRequestHeader struct {
	api_key        int16
	api_version    int16
	correlation_id uint32
}

func parseBasicRequestHeader(req []byte) *BasicRequestHeader {
	return &BasicRequestHeader{
		api_key:        int16(binary.BigEndian.Uint16(req[4:8])),
		api_version:    int16(binary.BigEndian.Uint16(req[6:8])),
		correlation_id: uint32(binary.BigEndian.Uint32(req[8:12])),
	}
}

/*
APIVersionResponse:
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
type APIVersionResponse struct {
	response_len     uint32
	correlation_id   uint32
	error_code       int16
	api_keys_len     int8
	api_keys         []API_Key
	throttle_time_ms uint32
	tagbuf           int8
}
type API_Key struct {
	api_key     int16
	min_version int16
	max_version int16
	tagbuf      int8
}

func newAPIVersionsResponse(id uint32, err int16, api_keys ...API_Key) APIVersionResponse {
	return APIVersionResponse{
		correlation_id: id,
		error_code:     err,
		api_keys:       api_keys,
		api_keys_len:   int8(len(api_keys)) + 1}
}
func newAPIKeys(apikey, min, max int16) API_Key {
	return API_Key{
		api_key:     apikey,
		min_version: min,
		max_version: max,
		tagbuf:      0}
}

func (api_version_response *APIVersionResponse) Encode() []byte {
	// response header
	buff := new(bytes.Buffer)
	binary.Write(buff, binary.BigEndian, api_version_response.response_len)
	binary.Write(buff, binary.BigEndian, api_version_response.correlation_id)
	binary.Write(buff, binary.BigEndian, api_version_response.error_code)
	binary.Write(buff, binary.BigEndian, api_version_response.api_keys_len)
	for _, key := range api_version_response.api_keys {
		err := binary.Write(buff, binary.BigEndian, key)
		if err != nil {
			fmt.Println(err)
			return []byte{}
		}
	}
	binary.Write(buff, binary.BigEndian, api_version_response.throttle_time_ms)
	binary.Write(buff, binary.BigEndian, api_version_response.tagbuf)
	return buff.Bytes()
}

func setMessageSize(res []byte) {
	binary.BigEndian.PutUint32(res[0:4], uint32(len(res)-4))
}
