# Subscribe Example

Build:
```
go build -o taoslogtail taoslogtail.go
go build -o taoslogpump taoslogpump.go
```

Run:

```
./taoslogtail
```

`taoslogtail` will subscribe table `log.log`, and `tail` the log. once new log item entered, it will 
print the log item as below:
```
May 28 04:39:01.374 0 ...:6030 user:... login from ..., result:success
```

You can generate above log via run say `taos -s exit`, or in batch
`while true; do taos -s exit; done`, but it's still not so many log
items generated. So you may need to run `taoslogpump`, to generate
more fake log items:

```
./taoslogpump
```


