package ldbio

import (
	"fmt"
	_ "net/http/pprof"
	"os"
	"time"
	"strings"
	"syscall"

	log "github.com/alecthomas/log4go"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

//leveldb管理
type LevelDBMser struct {
	filepath string
	ldb      *leveldb.DB
}

func NewLevelDBMgr(filepath string) *LevelDBMser {
	return &LevelDBMser{filepath: filepath}
}

func (this *LevelDBMser) Init() {
	var err error
	if this.ldb, err = leveldb.OpenFile(this.filepath, &opt.Options{
		OpenFilesCacheCapacity: 16,
		BlockCacheCapacity:     16 / 2 * opt.MiB,
		WriteBuffer:            16 / 4 * opt.MiB,
		Filter:                 filter.NewBloomFilter(10),
	}); err != nil {
		log.Info("NoInit start err:%v", err)
		time.Sleep(time.Second)
		os.Exit(1)
		return
	}
}
func (this *LevelDBMser) UnInit() {
	this.ldb.Close()
}

func (this *LevelDBMser) ScanPrefix(prefix string) map[string]string {
	var (
		ret  = make(map[string]string, 0)
		iter = this.ldb.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	)
	for iter.Next() {
		value := iter.Value()
		if value == nil {
			continue
		}
		ret[string(iter.Key())] = string(iter.Value())
	}
	iter.Release()
	if err := iter.Error(); err != nil {
		log.Warn("LevelDBMgr::loadDB err:%s", err)
		panic(fmt.Sprintf("LevelDBMgr LoadDB err:%v", err))
		if strings.Contains(err.Error(), "no space left on device") {
			panic(fmt.Sprintf("LevelDBMgr ScanPrefix key %v err %v", string(prefix), err))
			process, err := os.FindProcess(syscall.Getpid())
			if err != nil {
				log.Error("LevelDBMgr::ScanPrefix Stop App Find Process Failed: %v", err.Error())
				time.Sleep(time.Second * time.Duration(10))
				os.Exit(2)
			} else {
				//先发送ctrl+c等待5秒若未在kill
				process.Signal(syscall.SIGINT)
				time.Sleep(time.Second * time.Duration(10))
				log.Info("LevelDBMgr::ScanPrefix Force Kill!")
				time.Sleep(time.Millisecond * time.Duration(200))
				process.Kill()
			}
		}
	}
	return ret
}

func (this *LevelDBMser) Put(key, value []byte) {
	if err := this.ldb.Put(key, value, nil); err != nil {
		log.Error("LevelDBMgr Put key %v err %v", string(key), err)
		if strings.Contains(err.Error(), "no space left on device") {
			panic(fmt.Sprintf("LevelDBMgr Put key %v value %v err %v", string(key), string(value), err))
			process, err := os.FindProcess(syscall.Getpid())
			if err != nil {
				log.Error("LevelDBMgr::Put Stop App Find Process Failed: %v", err.Error())
				time.Sleep(time.Second * time.Duration(10))
				os.Exit(2)
			} else {
				//先发送ctrl+c等待5秒若未在kill
				process.Signal(syscall.SIGINT)
				time.Sleep(time.Second * time.Duration(10))
				log.Info("LevelDBMgr::Put Force Kill!")
				time.Sleep(time.Millisecond * time.Duration(200))
				process.Kill()
			}
		}
	}
}

func (this *LevelDBMser) Del(key []byte) {
	if err := this.ldb.Delete(key, nil); err != nil {
		log.Error("LevelDBMgr Del key(%v) err:%v", string(key), err)
		if strings.Contains(err.Error(), "no space left on device") {
			panic(fmt.Sprintf("LevelDBMgr Del key %v err %v", string(key), err))
			process, err := os.FindProcess(syscall.Getpid())
			if err != nil {
				log.Error("LevelDBMgr::Del Stop App Find Process Failed: %v", err.Error())
				time.Sleep(time.Second * time.Duration(10))
				os.Exit(2)
			} else {
				//先发送ctrl+c等待5秒若未在kill
				process.Signal(syscall.SIGINT)
				time.Sleep(time.Second * time.Duration(10))
				log.Info("LevelDBMgr::Del Force Kill!")
				time.Sleep(time.Millisecond * time.Duration(200))
				process.Kill()
			}
		}
	}
}

func (this *LevelDBMser) Get(key []byte) ([]byte, error) {
	return this.ldb.Get(key, nil)
}
