package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

const (
	Return   = "Return"
	Continue = "Continue"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	// 锁的名字 对应KVServer的key
	name string
	// 锁的id 每个线程唯一
	id string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.name = lockname
	lk.id = kvtest.RandValue(32)
	return lk
}

// 获取锁
func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.name)
		if err == rpc.OK {
			if value == "" {
				putErr := lk.ck.Put(lk.name, lk.id, version)
				next := acquirePutErr(putErr, lk, version)
				switch next {
				case Return:
					return
				case Continue:
					continue
				}
			} else {
				time.Sleep(time.Millisecond * 100)
				continue
			}
		} else if err == rpc.ErrNoKey {
			putErr := lk.ck.Put(lk.name, lk.id, 0)
			next := acquirePutErr(putErr, lk, version)
			switch next {
			case Return:
				return
			case Continue:
				continue
			}
		}
	}

}

// 释放锁
func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.name)
		if err == rpc.OK {
			if value == "" {
				return
			} else {
				if value == lk.id {
					putErr := lk.ck.Put(lk.name, "", version)
					next := releasePutErr(putErr, lk, version)
					switch next {
					case Return:
						return
					case Continue:
						continue
					}
				}
			}
		} else if err == rpc.ErrNoKey {
			return
		}
	}

}

func acquirePutErr(putErr rpc.Err, lk *Lock, version rpc.Tversion) string {
	if putErr == rpc.OK {
		return Return
	} else if putErr == rpc.ErrMaybe {
		value2, version2, _ := lk.ck.Get(lk.name)
		if value2 == lk.id && version2 == version+1 {
			return Return
		} else {
			return Continue
		}
	} else {
		time.Sleep(time.Millisecond * 100)
		return Continue
	}
}

func releasePutErr(putErr rpc.Err, lk *Lock, version rpc.Tversion) string {
	if putErr == rpc.OK {
		return Return
	} else if putErr == rpc.ErrMaybe {
		value2, version2, _ := lk.ck.Get(lk.name)
		if value2 == "" && version2 == version+1 {
			return Return
		} else {
			return Continue
		}
	} else {
		time.Sleep(time.Millisecond * 100)
		return Continue
	}
}
