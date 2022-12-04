package main

import (
	"errors"
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
	IO_BUF     int = 1024 * 16 // iobuf长度
	MAX_BULK   int = 1024 * 4  // bulk最长
	MAX_INLINE int = 1024 * 4  // 限制一个inline多长
)

type GoRedisDB struct {
	data   *Dict
	expire *Dict
}

type GoRedisServer struct {
	fd      int
	port    int
	db      *GoRedisDB
	clients map[int]*GoRedisClient
	aeLoop  *AeLoop
}

type GoRedisClient struct {
	fd       int
	db       *GoRedisDB
	args     []*GObj
	reply    *List
	sentLen  int // 一个GObj可能不能发送完毕，此时用于记录断点，用于下次发送，即记录replylist第一个元素已经发送的部分
	queryBuf []byte
	queryLen int // 未处理的命令的长度
	cmdTy    CmdType
	bulkNum  int
	bulkLen  int
}

type CommandProc func(c *GoRedisClient)

// do not support bulk command
type GoRedisCommand struct {
	name  string
	proc  CommandProc
	arity int
}

// Global Varibles
var server GoRedisServer

var cmdTable []GoRedisCommand = []GoRedisCommand{
	{"get", getCommand, 2},
	{"set", setCommand, 3},
}

func getCommand(c *GoRedisClient) {
	// TODO
}

func setCommand(c *GoRedisClient) {
	// TODO
}

func ProcessCommand(client *GoRedisClient) {
	//TODO: lookup command
	//TODO: call command
	//TODO: decrRef args
	resetClient(client)
}

func freeClient(client *GoRedisClient) {
	//TODO: delete file event
	//TODO: decrRef reply & args list
	//TODO: delete from clients
}

func resetClient(client *GoRedisClient) {
	client.cmdTy = COMMAND_UNKNOW
}

func (client *GoRedisClient) findLineInQuery() (int, error) {
	index := strings.Index(string(client.queryBuf[:client.queryLen]), "\r\n")
	if index < 0 && client.queryLen > MAX_INLINE {
		return index, errors.New("to bug inline cmd")
	}
	return index, nil
}

func (client *GoRedisClient) getNumInQuery(s, e int) (int, error) {
	num, err := strconv.Atoi(string(client.queryBuf[s:e]))
	client.queryBuf = client.queryBuf[e+2:]
	client.queryLen -= e + 2
	return num, err
}

func handleInlineBuf(client *GoRedisClient) (bool, error) {
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

func handleBulkBuf(client *GoRedisClient) (bool, error) {
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
			index, err := client.findLineInQuery()
			if index < 0 {
				return false, err
			}
			// 读出形如 $3r\r\nset\r\n
			if client.queryBuf[0] != '$' {
				return false, errors.New("expect $ for bulk length")
			}
			blen, err := client.getNumInQuery(1, index)
			if err != nil || blen == 0 {
				return false, err
			}
			if blen > MAX_BULK {
				return false, errors.New("too big bulk")
			}
			client.bulkLen = blen
		}
		if client.queryLen < client.bulkLen+2 {
			return false, nil
		}
		index := client.bulkLen
		if client.queryBuf[index] != '\r' || client.queryBuf[index+1] != '\n' {
			return false, errors.New("expect CRLF for bulk end")
		}
		// index, err := client.findLineInQuery()
		// if index < 0 {
		// 	return false, err
		// }
		// // 长度的应该和\r\n的位置呼应
		// if client.bulkLen != index {
		// 	return false, fmt.Errorf("expect bulk length %v, get %v", client.bulkLen, index)
		// }
		// bulkNum会迭代递减
		client.args[len(client.args)-client.bulkNum] = CreateObject(GSTR, string(client.queryBuf[:index]))
		client.queryBuf = client.queryBuf[index+2:]
		client.queryLen -= index + 2
		client.bulkLen = 0
		client.bulkNum -= 1
	}
	return true, nil
}

func ProcessQueryBuf(client *GoRedisClient) error {
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

func ReadQueryFromClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*GoRedisClient)
	// 装不下，进行扩容
	if len(client.queryBuf)-client.queryLen < MAX_BULK {
		client.queryBuf = append(client.queryBuf, make([]byte, MAX_BULK)...)
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
func SendReplyToClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*GoRedisClient)
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
				client.reply.DelNode(rep)
				// go 本身有gc是不用实现refcount的，这里是为了复现redis
				rep.Val.DecrRefCount()
				client.sentLen = 0
			} else {
				// 注意，write了不一定全部发送，真正n才是有效发送的，
				// 不等于说明fd的缓冲区满了
				break
			}
		}
	}
	if client.reply.Length() == 0 {
		client.sentLen = 0
		loop.RemoveFileEvent(fd, AE_WRITABLE)
	}
}

func GStrEqual(a, b *GObj) bool {
	if a.Type_ != GSTR || b.Type_ != GSTR {
		return false
	}
	return a.Val_.(string) == b.Val_.(string)
}

func GStrHash(key *GObj) int64 {
	if key.Type_ != GSTR {
		return 0
	}
	hash := fnv.New64()
	hash.Write([]byte(key.Val_.(string)))
	return int64(hash.Sum64())
}

func CreateClient(fd int) *GoRedisClient {
	return &GoRedisClient{
		fd:       fd,
		db:       server.db,
		queryBuf: make([]byte, IO_BUF),
		reply:    ListCreate(ListType{EqualFunc: GStrEqual}),
	}
}

func AcceptHandler(_ *AeLoop, fd int, extra interface{}) {
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
		entry := server.db.expire.RandomGet()
		if entry == nil {
			break
		}
		// expire dict 的 val 是时间戳
		if int64(entry.Val.IntVal()) < time.Now().Unix() {
			server.db.data.Delete(entry.Key)
			server.db.expire.Delete(entry.Key)
		}
	}
}

// initServer 初始化server
func initServer(config *Config) error {
	server.port = config.Port
	server.clients = make(map[int]*GoRedisClient)
	// 创建两个大字典，redis本身也是个大dict
	server.db = &GoRedisDB{
		data:   DictCreate(DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
		expire: DictCreate(DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
	}
	var err error
	// 创建ae事件
	if server.aeLoop, err = AeLoopCreate(); err != nil {
		return err
	}
	if server.fd, err = TcpServer(server.port); err != nil {
		return err
	}
	return nil
}

func main() {
	// 入参是配置文件地址
	path := os.Args[1]
	// 加载配置文件
	config, err := LoadConfig(path)
	if err != nil {
		log.Printf("config error: %v\n", err)
	}
	if err = initServer(config); err != nil {
		log.Printf("init server error: %v\n", err)
	}
	// 为server fd添加readable事件,该事件由AcceptHandler处理
	server.aeLoop.AddFileEvent(server.fd, AE_READABLE, AcceptHandler, nil)
	// 启动清除expire key 的事件
	server.aeLoop.AddTimeEvent(AE_NORMAL, 100, ServerCron, nil)
	log.Println("go-redis server is up.")
	server.aeLoop.AeMain()
}
