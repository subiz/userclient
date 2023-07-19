package userclient

import (
	"context"
	"sync"

	"github.com/subiz/header"
	cpb "github.com/subiz/header/common"
	"github.com/subiz/log"
)

var (
	readyLock *sync.Mutex
	ready     bool

	userc header.UserMgrClient
)

func init() {
	readyLock = &sync.Mutex{}
	conn := header.DialGrpc("user:12842", header.WithShardRedirect())
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

	user, err := GetUser(accid, userid)
	if err != nil {
		return nil, log.EServer(err, log.M{"account_id": accid, "user_id": userid})
	}

	if user.GetPrimaryId() != "" {
		primary, err := GetUser(accid, user.GetPrimaryId())
		if err != nil {
			return nil, log.EServer(err, log.M{"account_id": accid, "user_id": userid, "primary_id": user.GetPrimaryId()})
		}
		return primary, nil
	}
	return user, nil
}

// primary only, if pass in secondary => redirect to primary
func UpdateUserCtx(ctx *cpb.Context, u *header.User) error {
	if u == nil {
		return nil
	}
	waitUntilReady()
	_, err := userc.UpdateUser2(header.ToGrpcCtx(ctx), u)
	if err != nil {
		return log.EServer(err, log.M{"account_id": u.AccountId, "id": u.Id})
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
	out, err := userc.CreateUserEvent(header.ToGrpcCtx(ctx), ev)
	if err != nil {
		return nil, log.EServer(err, log.M{"account_id": accid, "user_id": userid, "event": ev})
	}
	return out, nil
}

func ScanUsers(accid string, cond *header.UserViewCondition, predicate func(users []*header.User, total int) bool) error {
	waitUntilReady()
	ctx := header.ToGrpcCtx(&cpb.Context{Credential: &cpb.Credential{AccountId: accid, Type: cpb.Type_subiz}})
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

func AddUserLabel(ctx context.Context, req *header.UserRequest) error {
	waitUntilReady()
	accid, userid, label := req.AccountId, req.UserId, req.ObjectId
	if ctx == nil {
		ctx = GenCtx(accid)
	}
	_, err := userc.AddUserLabel(ctx, req)
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
	return header.ToGrpcCtx(&cpb.Context{Credential: &cpb.Credential{AccountId: accid, Type: cpb.Type_subiz, Issuer: "subiz"}})
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
