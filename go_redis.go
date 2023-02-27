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
	bulkNum  int // multi模式下数组的长度
	bulkLen  int // multi模式下数组的子元素的长度
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
	{"expire", expireCommand, 3},
}

func getCommand(c *GoRedisClient) {
	key := c.args[1]
	val := findKeyRead(key)
	// 找有没有这个key,可能会过期
	if val == nil {
		// TODO: extract shared.strings
		c.AddReplyStr("$-1\r\n")
	} else if val.Type_ != GSTR {
		// TODO: extract shared.strings
		// 不是stirng类型，应该用其他的命令获取
		c.AddReplyStr("-ERR: wrong type\r\n")
	} else {
		str := val.StrVal()
		// 返回value
		c.AddReplyStr(fmt.Sprintf("$%d%v\r\n", len(str), str))
	}
}

func setCommand(c *GoRedisClient) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != GSTR {
		// TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type\r\n")
	}
	server.db.data.Set(key, val)
	server.db.expire.Delete(key)
	c.AddReplyStr("+OK\r\n")
}

func expireCommand(c *GoRedisClient) {
	key := c.args[1]
	val := c.args[2]
	if val.Type_ != GSTR {
		// TODO: extract shared.strings
		c.AddReplyStr("-ERR: wrong type\r\n")
	}
	// 转换成毫秒
	expire := GetMsTime() + (val.IntVal() * 1000)
	expObj := CreateFromInt(expire)
	server.db.expire.Set(key, expObj)
	expObj.DecrRefCount()
	c.AddReplyStr("+OK\r\n")
}

func findKeyRead(key *GObj) *GObj {
	expireIfNeeded(key)
	return server.db.data.Get(key)
}

// expireIfNeeded 检查是否已经过期
func expireIfNeeded(key *GObj) {
	entry := server.db.expire.Find(key)
	if entry == nil {
		return
	}
	when := entry.Val.IntVal()
	if when > GetMsTime() {
		return
	}
	_ = server.db.expire.Delete(key)
	_ = server.db.data.Delete(key)
}

func lookupCommand(cmdStr string) *GoRedisCommand {
	// TODO 忽略大小写
	for _, c := range cmdTable {
		if strings.EqualFold(c.name, cmdStr) {
			return &c
		}
	}
	return nil
}

func ProcessCommand(client *GoRedisClient) {
	cmdStr := client.args[0].StrVal()
	log.Printf("process command: %v\n", cmdStr)
	if cmdStr == "quit" {
		freeClient(client)
		return
	}
	defer resetClient(client)
	cmd := lookupCommand(cmdStr)
	if cmd == nil {
		client.AddReplyStr("-ERR: unknow command\r\n")
		return
	} else if cmd.arity != len(client.args) {
		client.AddReplyStr("-ERR: wrong number of args\r\n")
		return
	}
	cmd.proc(client)
}

func freeArgs(client *GoRedisClient) {
	for _, v := range client.args {
		v.DecrRefCount()
	}
}

func freeReplyList(client *GoRedisClient) {
	for client.reply.length != 0 {
		n := client.reply.head
		client.reply.DelNode(n)
		n.Val.DecrRefCount()
	}
}

func freeClient(client *GoRedisClient) {
	freeArgs(client)
	delete(server.clients, client.fd)
	server.aeLoop.RemoveFileEvent(client.fd, AE_READABLE)
	server.aeLoop.RemoveFileEvent(client.fd, AE_WRITABLE)
	freeReplyList(client)
	Close(client.fd)
}

func resetClient(client *GoRedisClient) {
	client.cmdTy = COMMAND_UNKNOW
}

// findLineInQuery 找到第一个\r\n的位置
func (c *GoRedisClient) findLineInQuery() (int, error) {
	index := strings.Index(string(c.queryBuf[:c.queryLen]), "\r\n")
	if index < 0 && c.queryLen > MAX_INLINE {
		return index, errors.New("to bug inline cmd")
	}
	return index, nil
}

func (c *GoRedisClient) getNumInQuery(s, e int) (int, error) {
	num, err := strconv.Atoi(string(c.queryBuf[s:e]))
	c.queryBuf = c.queryBuf[e+2:]
	c.queryLen -= e + 2
	return num, err
}

func (c *GoRedisClient) AddReply(o *GObj) {
	c.reply.Append(o)
	o.IncrRefCount()
	server.aeLoop.AddFileEvent(c.fd, AE_WRITABLE, SendReplyToClient, c)
}

func (c *GoRedisClient) AddReplyStr(str string) {
	o := CreateObject(GSTR, str)
	c.AddReply(o)
	o.DecrRefCount()
}

func handleInlineBuf(client *GoRedisClient) (bool, error) {
	index, err := client.findLineInQuery()
	// err是因为一个inline溢出,可能是发生了攻击
	if index < 0 {
		return false, err
	}
	// 用空格切分
	subs := strings.Split(string(client.queryBuf[:index]), " ")
	// 更新buf
	client.queryBuf = client.queryBuf[index+2:]
	client.queryLen -= index + 2
	// 把这段作为参数置入
	client.args = make([]*GObj, len(subs))
	for i, v := range subs {
		// 创建GObj
		client.args[i] = CreateObject(GSTR, v)
	}
	return true, nil
}

// handleBulkBuf 解析多行 eg:*3\r\n$3\r\nSet\r\n$3\r\nKey\r\n$3\r\nVal\r\n
func handleBulkBuf(client *GoRedisClient) (bool, error) {
	if client.bulkNum == 0 {
		// 找\r\n位置
		index, err := client.findLineInQuery()
		if index < 0 || err != nil {
			return false, err
		}
		// 把形如*3\r\n的数字读出来
		bnum, err := client.getNumInQuery(1, index)
		if err != nil {
			return false, err
		}
		// 数组元素为空
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
			// 读出形如 $3r\n\nset\r\n
			if client.queryBuf[0] != '$' {
				return false, errors.New("expect $ for bulk length")
			}
			// 该元素的长度,就是上面的3
			blen, err := client.getNumInQuery(1, index)
			if err != nil || blen == 0 {
				return false, err
			}
			// 单条bulkLen的长度
			if blen > MAX_BULK {
				return false, errors.New("too big bulk")
			}
			client.bulkLen = blen
		}
		// 可能未完全接受，例如$3\r\nSe
		if client.queryLen < client.bulkLen+2 {
			return false, nil
		}
		// 接受该bulk缓存
		index := client.bulkLen
		if client.queryBuf[index] != '\r' || client.queryBuf[index+1] != '\n' {
			return false, errors.New("expect CRLF for bulk end")
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

// ProcessQueryBuf 处理命令
func ProcessQueryBuf(client *GoRedisClient) error {
	// 当有未处理的命令时
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
			// 命令不完整
			break
		}
	}
	return nil
}

func ReadQueryFromClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*GoRedisClient)
	// 装不下，进行扩容
	// 这里减出来的空间用于放一条bulk命令，为了保证能够至少放得下，需要提前扩容
	if len(client.queryBuf)-client.queryLen < MAX_BULK {
		client.queryBuf = append(client.queryBuf, make([]byte, MAX_BULK)...)
	}
	// queryLen前面还没有处理，不允许覆盖
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
	// 增加未处理命令的长度
	client.queryLen += n
	log.Printf("read %v bytes from client:%v\n", n, client.fd)
	log.Printf("ReadQueryFromClient, queryBuf : %v\n", string(client.queryBuf))
	if err = ProcessQueryBuf(client); err != nil {
		log.Printf("process query buf err: %v\n", err)
		return
	}
}

func SendReplyToClient(loop *AeLoop, fd int, extra interface{}) {
	client := extra.(*GoRedisClient)
	log.Printf("SendReplyToClient, reply len:%v\n", client.reply.Length())
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
			log.Printf("send %v bytes to client:%v\n", n, client.fd)
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
	if _, err := hash.Write([]byte(key.Val_.(string))); err != nil {
		return 0
	}
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
	cfd, err := Accept(fd)
	if err != nil {
		log.Printf("accept err: %v\n", err)
		return
	}
	client := CreateClient(cfd)
	// TODO: check max clients limit
	server.clients[cfd] = client
	server.aeLoop.AddFileEvent(cfd, AE_READABLE, ReadQueryFromClient, client)
	log.Printf("accept client, fd: %v\n", cfd)
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
		if entry.Val.IntVal() < time.Now().Unix() {
			_ = server.db.data.Delete(entry.Key)
			_ = server.db.expire.Delete(entry.Key)
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
	path := ""
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	// 加载配置文件
	config, err := LoadConfig(path)
	if err != nil {
		log.Printf("config error: %v\n", err)
	}
	if err = initServer(config); err != nil {
		log.Printf("init server error: %v\n", err)
		return
	}
	// 为server fd添加readable事件,该事件由AcceptHandler处理
	server.aeLoop.AddFileEvent(server.fd, AE_READABLE, AcceptHandler, nil)
	// 启动清除expire key 的事件
	server.aeLoop.AddTimeEvent(AE_NORMAL, 100, ServerCron, nil)
	log.Println("go-redis server is up.")
	log.Println(`     
	____   ____           _______   ____   __| _/|__| ______
   / ___\ /  _ \   ______ \_  __ \_/ __ \ / __ | |  |/  ___/
  / /_/  >  <_> ) /_____/  |  | \/\  ___// /_/ | |  |\___ \ 
  \___  / \____/           |__|    \___  >____ | |__/____  >
 /_____/                               \/     \/         \/ 
 `)
	server.aeLoop.AeMain()
}
