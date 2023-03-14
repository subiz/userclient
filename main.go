package userclient

import (
	"context"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/subiz/goutils/clock"
	"github.com/subiz/header"
	cpb "github.com/subiz/header/common"
	"github.com/subiz/idgen"
	"github.com/subiz/sgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

var (
	readyLock *sync.Mutex
	ready     bool

	cqlsession *gocql.Session
	userc      header.UserMgrClient
)

func init() {
	readyLock = &sync.Mutex{}
}

func initialize() {
	userservice := "user:12842"
	conn, err := grpc.Dial(userservice, grpc.WithTransportCredentials(insecure.NewCredentials()), sgrpc.WithShardRedirect())
	if err != nil {
		panic(err)
	}
	userc = header.NewUserMgrClient(conn)
	cluster := gocql.NewCluster("db-0")
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	cluster.Keyspace = "user"
	cqlsession, err = cluster.CreateSession()
	if err != nil {
		panic(err)
	}
}

func waitUntilReady() {
	if ready {
		return
	}
	readyLock.Lock()
	if ready {
		readyLock.Unlock()
		return
	}
	initialize()
	ready = true
	readyLock.Unlock()
}

func GetUser(accid, userid string) (*header.User, error) {
	waitUntilReady()
	user, err := userc.ReadUser(context.Background(), &header.Id{AccountId: accid, Id: userid})
	if err == nil {
		return user, nil
	}

	u := &header.User{AccountId: accid, Id: userid}
	ub := make([]byte, 0)
	created, _ := idgen.GetCreated(userid, idgen.USER_PREFIX)
	hour := clock.UnixHour(created)
	err = cqlsession.Query(`SELECT attrs FROM users WHERE account_id=? AND hour=? AND id=?`, accid, hour, userid).Scan(&ub)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		return u, nil
	}

	if err != nil {
		return nil, header.E500(err, header.E_database_error, accid)
	}

	if err := proto.Unmarshal(ub, u); err != nil {
		return nil, header.E500(err, header.E_invalid_proto, accid, userid)
	}
	return u, nil
}

// primary only, if pass in secondary => redirect to primary
func UpdateUser(u *header.User) error {
	ctx := &cpb.Context{Credential: &cpb.Credential{AccountId: u.AccountId, Type: cpb.Type_subiz}}
	return UpdateUserCtx(ctx, u)
}

// primary only, if pass in secondary => redirect to primary
func UpdateUserCtx(ctx *cpb.Context, u *header.User) error {
	if u == nil {
		return nil
	}
	waitUntilReady()
	_, err := userc.UpdateUser2(sgrpc.ToGrpcCtx(ctx), u)
	if err != nil {
		return header.E500(err, header.E_subiz_call_failed, u.GetAccountId(), u.GetId())
	}
	return nil
}

// update using current id (dont redirect to primary)
func UpdateUserPlain(accid, id string, attributes []*header.Attribute) error {
	ctx := &cpb.Context{Credential: &cpb.Credential{AccountId: accid, Type: cpb.Type_subiz}}
	return UpdateUserPlainCtx(ctx, accid, id, attributes)
}

// update using current id (dont redirect to primary)
func UpdateUserPlainCtx(ctx *cpb.Context, accid, id string, attributes []*header.Attribute) error {
	waitUntilReady()
	_, err := userc.UpdateUser2(sgrpc.ToGrpcCtx(ctx), &header.User{
		AccountId:  accid,
		Id:         id,
		Attributes: attributes,
		PrimaryId:  id, // special mark
	})
	if err != nil {
		return header.E500(err, header.E_subiz_call_failed, accid, id)
	}
	return nil
}

func GetOrCreateUserByProfile(accid, channel, source, profileid string) (*header.User, error) {
	waitUntilReady()
	ctx := GenCtx(accid)
	u, err := userc.ReadOrCreateUserByContactProfile(ctx, &header.Id{
		AccountId:     accid,
		Channel:       channel,
		ChannelSource: source,
		ProfileId:     profileid,
	})
	if err != nil {
		return nil, header.E500(err, header.E_subiz_call_failed, accid)
	}
	return u, nil
}

func CreateEvent(ctx *cpb.Context, accid, userid string, ev *header.Event) (*header.Event, error) {
	waitUntilReady()
	ev, err := userc.CreateUserEvent(sgrpc.ToGrpcCtx(ctx), ev)
	if err != nil {
		return nil, header.E500(err, header.E_subiz_call_failed, accid, userid)
	}
	return ev, nil
}

func ScanUsers(accid string, cond *header.UserViewCondition, predicate func(users []*header.User, total int) bool) error {
	waitUntilReady()
	ctx := sgrpc.ToGrpcCtx(&cpb.Context{Credential: &cpb.Credential{AccountId: accid, Type: cpb.Type_subiz}})
	// max 50 M lead
	anchor := ""
	for i := 0; i < 1_000; i++ {
		out, err := userc.ListUsers(ctx, &header.ListUserRequest{
			AccountId: accid,
			Condition: cond,
			Anchor:    anchor,
			Limit:     50,
		})
		if err != nil {
			return err
		}
		if out.Anchor == "" || anchor == out.Anchor || len(out.GetUsers()) == 0 {
			break
		}
		anchor = out.Anchor
		if !predicate(out.GetUsers(), int(out.GetTotal())) {
			break
		}
	}
	return nil
}

func ListSegmentUserIds(accid, segmentid string, f func(string) bool) error {
	waitUntilReady()
	ctx := GenCtx(accid)
	for i := 0; i < 50; i++ { // NPartition
		ids, err := userc.ListSegmentUserIds(ctx, &header.ListUserRequest{
			AccountId: accid,
			SegmentId: segmentid,
			OrderBy:   segmentid,
			Partition: int64(i),
		})
		if err != nil {
			return err
		}
		for _, id := range ids.Ids {
			if !f(id) {
				break
			}
		}
	}
	return nil
}

func ListSegmentUsers(accid string, segments, excludeSegments []string, f func(*header.User) bool) error {
	waitUntilReady()
	ctx := GenCtx(accid)
	users, err := userc.ListAllSegmentUsers(ctx, &header.ListUserRequest{
		AccountId:       accid,
		Segments:        segments,
		ExcludeSegments: excludeSegments,
	})
	if err != nil {
		return err
	}
	for _, user := range users.GetUsers() {
		if !f(user) {
			break
		}
	}
	return nil
}

func UpsertSegment(segment *header.Segment) error {
	waitUntilReady()
	accid := segment.GetAccountId()
	ctx := GenCtx(accid)
	if _, err := userc.CreateSegment(ctx, segment); err != nil {
		return header.E500(err, header.E_subiz_call_failed, accid, "UPSERT SEGMENT")
	}
	return nil
}

func AddUserToSegment(accid, segmentid string, userid []string) error {
	waitUntilReady()
	ctx := GenCtx(accid)
	_, err := userc.AddToSegment(ctx, &header.SegmentUsers{
		AccountId: accid,
		SegmentId: segmentid,
		UserIds:   userid,
	})
	if err != nil {
		return header.E500(err, header.E_subiz_call_failed, accid, "ADD TO SEGMENT")
	}
	return nil
}

func RemoveUserFromSegment(accid, segmentid string, userids []string) error {
	waitUntilReady()
	ctx := GenCtx(accid)
	_, err := userc.RemoveFromSegment(ctx, &header.SegmentUsers{
		AccountId: accid,
		SegmentId: segmentid,
		UserIds:   userids,
	})
	if err != nil {
		return header.E500(err, header.E_subiz_call_failed, accid, "REMOVE FROM SEGMENT")
	}
	return nil
}

func AddUserLabel(accid, userid, label string) error {
	waitUntilReady()
	ctx := GenCtx(accid)
	_, err := userc.AddUserLabel(ctx, &header.UserRequest{
		AccountId: accid,
		UserId:    userid,
		ObjectId:  label,
	})
	if err != nil {
		return header.E500(err, header.E_subiz_call_failed, accid, "ADD USER LABEL")
	}
	return nil
}

func RemoveUserLabel(accid, userid, label string) error {
	waitUntilReady()
	ctx := GenCtx(accid)
	_, err := userc.RemoveUserLabel(ctx, &header.UserRequest{
		AccountId: accid,
		UserId:    userid,
		ObjectId:  label,
	})
	if err != nil {
		return header.E500(err, header.E_subiz_call_failed, accid, "REMOVE USER LABEL")
	}
	return nil
}

func GenCtx(accid string) context.Context {
	return sgrpc.ToGrpcCtx(&cpb.Context{Credential: &cpb.Credential{AccountId: accid, Type: cpb.Type_subiz, Issuer: "subiz"}})
}

func AddLeadOwner(accid, userid, agentid string) error {
	ctx := GenCtx(accid)
	_, err := userc.AddLeadOwner(ctx, &header.UserRequest{
		AccountId: accid,
		UserId:    userid,
		ObjectId:  agentid,
	})
	if err != nil {
		return header.E500(err, header.E_subiz_call_failed, accid, "CANNOT ADD LEAD OWNER")
	}
	return nil
}
