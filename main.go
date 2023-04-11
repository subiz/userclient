package userclient

import (
	"context"
	"sync"

	"github.com/subiz/header"
	cpb "github.com/subiz/header/common"
	"github.com/subiz/log"
	"github.com/subiz/sgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	readyLock *sync.Mutex
	ready     bool

	userc header.UserMgrClient
)

func init() {
	readyLock = &sync.Mutex{}
	conn, err := grpc.Dial("user:12842", grpc.WithTransportCredentials(insecure.NewCredentials()), sgrpc.WithShardRedirect())
	if err != nil {
		panic(err)
	}
	userc = header.NewUserMgrClient(conn)
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
	ready = true
	readyLock.Unlock()
}

func GetUser(accid, userid string) (*header.User, error) {
	waitUntilReady()

	user, err := userc.ReadUser(GenCtx(accid), &header.Id{AccountId: accid, Id: userid})
	if err != nil {
		return nil, log.EServer(err, log.M{"account_id": accid, "user_id": userid})
	}

	return user, nil
}

func GetPrimaryUser(accid, userid string) (*header.User, error) {
	waitUntilReady()
	user, err := userc.ReadUser(GenCtx(accid), &header.Id{AccountId: accid, Id: userid})
	if err != nil {
		return nil, log.EServer(err, log.M{"account_id": accid, "user_id": userid})
	}

	if user.GetPrimaryId() != "" {
		primary, err := userc.ReadUser(GenCtx(accid), &header.Id{AccountId: accid, Id: userid})
		if err != nil {
			return nil, log.EServer(err, log.M{"account_id": accid, "user_id": userid, "primary_id": user.GetPrimaryId()})
		}
		return primary, nil
	}
	return user, nil
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
		return log.EServer(err, log.M{"account_id": u.AccountId, "id": u.Id})
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
		return log.EServer(err, log.M{"account_id": accid, "id": id})
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
		return nil, log.EServer(err, log.M{"account_id": accid, "channel": channel, "source": source, "profile_id": profileid})
	}
	return u, nil
}

func CreateEvent(ctx *cpb.Context, accid, userid string, ev *header.Event) (*header.Event, error) {
	waitUntilReady()
	out, err := userc.CreateUserEvent(sgrpc.ToGrpcCtx(ctx), ev)
	if err != nil {
		return nil, log.EServer(err, log.M{"account_id": accid, "user_id": userid, "event": ev})
	}
	return out, nil
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
		return log.EServer(err, log.M{"segment": segment})
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
		return log.EServer(err, log.M{"account_id": accid, "segment": segmentid, "userid": userid})
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
		return log.EServer(err, log.M{"account_id": accid, "segment": segmentid, "userid": userids})
	}
	return nil
}

func UpsertLabel(label *header.Label) error {
	waitUntilReady()
	ctx := GenCtx(label.AccountId)
	_, err := userc.UpsertLabel(ctx, label)
	if err != nil {
		return log.EServer(err, log.M{"label": label})
	}
	return nil
}

func ListAllLabels(accid string) ([]*header.Label, error) {
	waitUntilReady()
	ctx := GenCtx(accid)
	labels, err := userc.ListLabels(ctx, &header.Id{AccountId: accid, Id: accid})
	if err != nil {
		return nil, log.EServer(err, log.M{"account_id": accid})
	}
	return labels.GetLabels(), nil
}

func AddUserLabel(ctx context.Context, accid, userid, label string) error {
	waitUntilReady()
	if ctx == nil {
		ctx = GenCtx(accid)
	}
	_, err := userc.AddUserLabel(ctx, &header.UserRequest{
		AccountId: accid,
		UserId:    userid,
		ObjectId:  label,
	})
	if err != nil {
		return log.EServer(err, log.M{"account_id": accid, "user_id": userid, "label": label})
	}
	return nil
}

func RemoveUserLabel(ctx context.Context, accid, userid, label string) error {
	waitUntilReady()
	if ctx == nil {
		ctx = GenCtx(accid)
	}
	_, err := userc.RemoveUserLabel(ctx, &header.UserRequest{
		AccountId: accid,
		UserId:    userid,
		ObjectId:  label,
	})
	if err != nil {
		return log.EServer(err, log.M{"account_id": accid, "user_id": userid, "label": label})
	}
	return nil
}

func GenCtx(accid string) context.Context {
	return sgrpc.ToGrpcCtx(&cpb.Context{Credential: &cpb.Credential{AccountId: accid, Type: cpb.Type_subiz, Issuer: "subiz"}})
}

func AddLeadOwner(accid, userid, agentid string) error {
	waitUntilReady()
	ctx := GenCtx(accid)
	_, err := userc.AddLeadOwner(ctx, &header.UserRequest{
		AccountId: accid,
		UserId:    userid,
		ObjectId:  agentid,
	})
	if err != nil {
		return log.EServer(err, log.M{"account_id": accid, "user_id": userid, "agent_id": agentid})
	}
	return nil
}
