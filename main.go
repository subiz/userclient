package userclient

import (
	"context"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/subiz/errors"
	"github.com/subiz/goutils/clock"
	"github.com/subiz/header"
	cpb "github.com/subiz/header/common"
	pb "github.com/subiz/header/user"
	"github.com/subiz/idgen"
	"github.com/subiz/sgrpc"
	"google.golang.org/grpc"
)

var (
	readyLock *sync.Mutex
	ready     bool

	cqlsession *gocql.Session
	cache      *ristretto.Cache
	userc      header.UserMgrClient
	eventc     header.EventMgrClient
)

func Init(userservice string, seeds []string) {
	readyLock = &sync.Mutex{}
	go func() {
		readyLock.Lock()

		conn, err := grpc.Dial(userservice, grpc.WithInsecure(), sgrpc.WithShardRedirect())
		if err != nil {
			panic(err)
		}
		userc = header.NewUserMgrClient(conn)
		eventc = header.NewEventMgrClient(conn)

		cluster := gocql.NewCluster(seeds...)
		cluster.Timeout = 10 * time.Second
		cluster.Keyspace = "users2users"
		cqlsession, err = cluster.CreateSession()
		if err != nil {
			panic(err)
		}

		cache, err = ristretto.NewCache(&ristretto.Config{
			NumCounters: 1e4, // number of keys to track frequency of (10k).
			MaxCost:     1e8, // maximum cost of cache (100MB).
			BufferItems: 64,  // number of keys per Get buffer.
		})
		if err != nil {
			panic(err)
		}
		ready = true
		readyLock.Unlock()
	}()
}

func waitUntilReady() {
	if ready {
		return
	}
	readyLock.Lock()
	readyLock.Unlock()
}

func GetUser(accid, userid string) (*pb.User, error) {
	waitUntilReady()
	// read in cache
	if value, found := cache.Get(accid + userid); found {
		if value == nil {
			return &pb.User{AccountId: accid, Id: userid}, nil
		}
		return value.(*pb.User), nil
	}

	user, err := userc.ReadUser(context.Background(), &cpb.Id{AccountId: accid, Id: userid})
	if err == nil {
		cache.SetWithTTL(accid+userid, user, 1000, 30*time.Second)
		return user, nil
	}

	u := &pb.User{AccountId: accid, Id: userid}
	ub := make([]byte, 0)
	created, _ := idgen.GetCreated(userid, idgen.USER_PREFIX)
	hour := clock.UnixHour(created)
	err = cqlsession.Query(`SELECT attrs FROM users WHERE account_id=? AND hour=? AND id=?`, accid, hour, userid).Scan(&ub)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.SetWithTTL(accid+userid, u, 1000, 30*time.Second)
		return u, nil
	}

	if err != nil {
		return nil, errors.Wrap(err, 500, errors.E_database_error, accid)
	}

	if err := proto.Unmarshal(ub, u); err != nil {
		return nil, errors.Wrap(err, 500, errors.E_proto_marshal_error, accid, userid)
	}

	cache.SetWithTTL(accid+userid, u, 1000, 30*time.Second)
	return u, nil
}

func SetUser(ctx *cpb.Context, u *pb.User) error {
	_, err := userc.UpdateUser(sgrpc.ToGrpcCtx(ctx), u)
	return err
}

func CreateEvent(ctx *cpb.Context, accid, userid string, ev *pb.Event) error {
	_, err := eventc.CreateEvent(sgrpc.ToGrpcCtx(ctx), &pb.UserEvent{
		AccountId: accid,
		UserId:    userid,
		Event:     ev,
	})
	return err
}
