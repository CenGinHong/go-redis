# go-redis
按照古早版本的redis仿写的golang的tiny版本


## Get Started
```dockerfile
docker build -t go-redis. 
```

```shell
docker run -itd -p 8787:8787 --name=go-redis go-redis 
```

使用telnet命令连接
```shell
telnet localhost 8787 
set k1 v1
get k1
```

