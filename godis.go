package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type CmdType = byte

const (
	COMMAND_UNKNOW CmdType = 0x00
	COMMAND_INLINE CmdType = 0x01
	COMMAND_BULK   CmdType = 0x02
)

const (
	GODIS_IO_BUF     int = 1024 * 16 // iobuf长度
	GODIS_MAX_BULK   int = 1024 * 4  // bulk最长
	GODIS_MAX_INLINE int = 1024 * 4  // 限制一个inline多长
)

type GodisDB struct {
	data   *Dict
	expire *Dict
}

type GodisServer struct {
	fd      int
	port    int
	db      *GodisDB
	clients map[int]*GodisClient
	aeLoop  *AeLoop
}

type GodisClient struct {
	fd       int
	db       *GodisDB
	args     []*GObj
	reply    *List
	sentLen  int // 一个GObj可能不能发送完毕，此时用于记录断点，用于下次发送，即记录replylist第一个元素已经发送的部分
	queryBuf []byte
	queryLen int // 未处理的命令的长度
	cmdTy    CmdType
	bulkNum  int
	bulkLen  int
}

// Global Varibles
var server GodisServer

func ProcessCommand(client *GodisClient) {
	//TODO: lookup command
	//TODO: call command
	//TODO: decrRef args
}

func freeClient(client *GodisClient) {
	//TODO: delete file event
	//TODO: decrRef reply & args list
	//TODO: delete from clients
}

func resetClient(client *GodisClient) {

}

func (client *GodisClient) findLineInQuery() (int, error) {
	index := strings.IndexAny(string(client.queryBuf[:client.queryLen]), "\r\n")
	if index < 0 && client.queryLen > GODIS_MAX_INLINE {
		return index, errors.New("to bug inline cmd")
	}
	return index, nil
}

func (client *GodisClient) getNumInQuery(s, e int) (int, error) {
	num, err := strconv.Atoi(string(client.queryBuf[s:e]))
	client.queryBuf = client.queryBuf[e+2:]
	client.queryLen -= e + 2
	return num, err
}

func handleInlineBuf(client *GodisClient) (bool, error) {
	index, err := client.findLineInQuery()
	// err是因为一个inline溢出,可能是发生了攻击
	if index < 0 {
		return false, err
	}
	subs := strings.Split(string(client.queryBuf[:index]), " ")
	client.queryBuf = client.queryBuf[index+2:]
	client.queryLen -= index + 2
	client.args = make([]*GObj, len(subs))
	for i, v := range subs {
		client.args[i] = CreateObject(GSTR, v)
	}
	return true, nil
}

func handleBulkBuf(client *GodisClient) (bool, error) {
	if client.bulkNum == 0 {
		// 说明还没有读出来
		index, err := client.findLineInQuery()
		if index < 0 {
			return false, err
		}
		// 把形如*3\r\n的数字且出来
		bnum, err := client.getNumInQuery(1, index)
		if err != nil {
			return false, err
		}
		if bnum == 0 {
			return true, nil
		}
		client.bulkNum = bnum
		client.args = make([]*GObj, client.bulkNum)
	}
	for client.bulkNum > 0 {
		// read bulk length
		if client.bulkLen == 0 {
			// 读出形如 $3r\r\nset\r\n
			if client.queryBuf[0] != '$' {
				return false, errors.New("expect $ for bulk length")
			}
			index, err := client.findLineInQuery()
			if index < 0 {
				return false, err
			}
			blen, err := client.getNumInQuery(1, index)
			if err != nil || blen == 0 {
				return false, err
			}
			client.bulkLen = blen
		}
		index, err := client.findLineInQuery()
		if index < 0 {
			return false, err
		}
		// 长度的应该和\r\n的位置呼应
		if client.bulkLen != index {
			return false, fmt.Errorf("expect bulk length %v, get %v", client.bulkLen, index)
		}
		// bulkNum会迭代递减
		client.args[len(client.args)-client.bulkNum] = CreateObject(GSTR, string(client.queryBuf[:index]))
		client.queryBuf = client.queryBuf[index+2:]
		client.queryLen -= index + 2
		client.bulkLen = 0
		client.bulkNum -= 1
	}
	return true, nil
}

func ProcessQueryBuf(client *GodisClient) error {
	for client.queryLen > 0 {
		if client.cmdTy == COMMAND_UNKNOW {
			if client.queryBuf[0] == '*' {
				client.cmdTy = COMMAND_BULK
			} else {
				client.cmdTy = COMMAND_INLINE
			}
		}
		var ok bool
		var err error
		// ok表示这个buff是否完整的命令，err表示执行是否出错
		if client.cmdTy == COMMAND_INLINE {
			ok, err = handleInlineBuf(client)
		} else if client.cmdTy == COMMAND_BULK {
			ok, err = handleBulkBuf(client)
		} else {
			return errors.New("unknow go-redis command Type")
		}
		if err != nil {
			return err
		}
		if ok {
			if len(client.args) == 0 {
				resetClient(client)
			} else {
				ProcessCommand(client)
			}
		} else {
			// cmd incompelete
			break
		}
	}
	return nil
}

func ReadQueryFromClient(loop *AeLoop, fd int, extra any) {
	client := extra.(*GodisClient)
	// 装不下，进行扩容
	if len(client.queryBuf)-client.queryLen < GODIS_MAX_BULK {
		client.queryBuf = append(client.queryBuf, make([]byte, GODIS_MAX_BULK)...)
	}
	n, err := Read(fd, client.queryBuf[client.queryLen:])
	if err != nil {
		log.Printf("client %v read err: %v\n", fd, err)
		return
	}
	defer func() {
		if err != nil {
			freeClient(client)
		}
	}()
	client.queryLen += n
	if err = ProcessQueryBuf(client); err != nil {
		log.Printf("process query buf err: %v\n", err)
		return
	}
}
func SendReplyToClient(loop *AeLoop, fd int, extra any) {
	client := extra.(*GodisClient)
	for client.reply.Length() > 0 {
		// 取第一个元素
		rep := client.reply.First()
		buf := []byte(rep.Val.Val_.(string))
		bufLen := len(buf)
		if client.sentLen < bufLen {
			n, err := Write(fd, buf[client.sentLen:])
			if err != nil {
				log.Printf("send reply err: %v\n", err)
				freeClient(client)
				return
			}
			client.sentLen += n
			// 完全发送完
			if client.sentLen == bufLen {
				client.reply.Delete(rep.Val)
				// go 本身有gc是不用实现refcount的，这里是为了复现redis
				rep.Val.DecrRefCount()
				client.sentLen = 0
			} else {
				// 注意，write了不一定全部发送，真正n才是有效发送的，
				// 不等于说明fd的缓冲区满了
				break;
			}
		}
	}
	if client.reply.Length() == 0 {
		client.sentLen = 0
		loop.RemoveFileEvent(fd,AE_WRITABLE)
	}
}

func GStrEqual(a, b *GObj) bool {
	if a.Type_ != GSTR || b.Type_ != GSTR {
		return false
	}
	return a.Val_.(string) == b.Val_.(string)
}

func GStrHash(key *GObj) int {
	if key.Type_ != GSTR {
		return 0
	}
	hash := fnv.New32()
	hash.Write([]byte(key.Val_.(string)))
	return int(hash.Sum32())
}

func CreateClient(fd int) *GodisClient {
	return &GodisClient{
		fd:       fd,
		db:       server.db,
		queryBuf: make([]byte, GODIS_IO_BUF),
		reply:    ListCreate(ListType{EqualFunc: GStrEqual}),
	}
}

func AcceptHandler(_ *AeLoop, fd int, extra any) {
	nfd, err := Accept(fd)
	if err != nil {
		log.Printf("accept err: %v\n", err)
		return
	}
	client := CreateClient(nfd)
	// TODO: check max clients limit
	server.clients[fd] = client
	server.aeLoop.AddFileEvent(fd, AE_READABLE, ReadQueryFromClient, client)
}

const EXPIRE_CHECK_COUNT int = 100

func ServerCron(_ *AeLoop, id int, extra interface{}) {
	// 随机检查100个在expire字典的key
	for i := 0; i < EXPIRE_CHECK_COUNT; i++ {
		key, val := server.db.expire.RandomGet()
		if key == nil {
			break
		}
		if int64(val.IntVal()) < time.Now().Unix() {
			server.db.data.RemoveKey(key)
			server.db.expire.RemoveKey(key)
		}
	}
}

func initServer(config *Config) error {
	server.port = config.Port
	server.clients = make(map[int]*GodisClient)
	server.db = &GodisDB{
		data:   DictCreate(DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
		expire: DictCreate(DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
	}
	var err error
	if server.aeLoop, err = AeLoopCreate(); err != nil {
		return err
	}
	server.fd, err = TcpServer(server.port)
	return err
}

func main() {
	// 入参并加载为配置文件
	path := os.Args[1]
	config, err := LoadConfig(path)
	if err != nil {
		log.Printf("config error: %v\n", err)
	}
	if err = initServer(config); err != nil {
		log.Printf("init server error: %v\n", err)
	}
	// 为server fd添加事件
	server.aeLoop.AddFileEvent(server.fd, AE_READABLE, AcceptHandler, nil)
	// 启动清除expire key 的事件
	server.aeLoop.AddTimeEvent(AE_NORMAL, 100, ServerCron, nil)
	log.Println("godis server is up.")
	server.aeLoop.AeMain()
}
