package test

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wzshiming/lrdb"
	client "github.com/wzshiming/lrdb/client/lrdb"
	"github.com/wzshiming/lrdb/engine/leveldb"
	"github.com/wzshiming/lrdb/reply"
	"github.com/wzshiming/resp"
)

var testAddress = "127.0.0.1:60101"

func init() {
	db, err := leveldb.NewLevelDBWithMemStorage()
	if err != nil {
		panic(err)
	}

	go func() {
		err := lrdb.NewLRDB(db.Cmd()).Listen(testAddress)
		if err != nil {
			panic(err)
		}
	}()
	time.Sleep(time.Second / 100)
}

type command struct {
	command []string
	want    resp.Reply
	wantErr bool
}

func TestGetSetDel(t *testing.T) {
	sets := []command{}
	gets := []command{}
	keys := []string{}
	for i := 0; i != 2; i++ {
		r := rand.Int()
		key := fmt.Sprintf("set_key_%d", r)
		val := fmt.Sprintf("set_data_%d", r)
		sets = append(sets, command{
			[]string{"set", key, val}, reply.OK, false,
		})
		gets = append(gets, command{
			[]string{"get", key}, resp.ReplyBulk(val), false,
		})
		keys = append(keys, key)
	}
	tests := append(sets, gets...)

	sort.StringsAreSorted(keys)
	keysV, _ := resp.ConvertTo(keys)
	tests = append(tests, command{
		[]string{"keys", "", "", strconv.FormatInt(int64(len(keys)), 10)}, keysV, false,
	})

	size, _ := resp.ConvertTo(len(keys))
	tests = append(tests, command{
		append([]string{"del"}, keys...), size, false,
	})
	testCommand(t, "getsetdel", tests)
}

func TestRename(t *testing.T) {
	tests := []command{
		{[]string{"exists", "hello"}, reply.Zero, false},
		{[]string{"set", "hello", "world"}, reply.OK, false},
		{[]string{"exists", "hello"}, reply.One, false},
		{[]string{"get", "hello"}, resp.ReplyBulk("world"), false},
		{[]string{"rename", "hello", "hello2"}, reply.OK, false},
		{[]string{"exists", "hello"}, reply.Zero, false},
		{[]string{"exists", "hello2"}, reply.One, false},
		{[]string{"get", "hello2"}, resp.ReplyBulk("world"), false},
		{[]string{"del", "hello2"}, reply.One, false},
		{[]string{"exists", "hello"}, reply.Zero, false},
		{[]string{"exists", "hello2"}, reply.Zero, false},
	}
	testCommand(t, "rename", tests)
}

func testCommand(t *testing.T, name string, command []command) {
	cli, err := client.NewClient(testAddress)
	if err != nil {
		t.Error(err)
		return
	}
	defer cli.Close()

	for _, tt := range command {
		cmd := strings.Join(tt.command, " ")
		t.Run(name+" "+cmd, func(t *testing.T) {
			got, err := cli.Command(tt.command[0], tt.command[1:]...)
			if (err != nil) != tt.wantErr {
				t.Errorf("'%v' error = %v, wantErr %v", cmd, err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("'%v' = %v, want %v", cmd, got.Format(0), tt.want.Format(0))
			}
		})
	}
}
