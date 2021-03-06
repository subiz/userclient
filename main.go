package userclient

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/gocql/gocql"
	"github.com/golang/protobuf/proto"
	"github.com/subiz/goutils/clock"
	"github.com/subiz/header"
	cpb "github.com/subiz/header/common"
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

func Init(userservice string) {
	readyLock = &sync.Mutex{}
	go func() {
		readyLock.Lock()

		conn, err := grpc.Dial(userservice, grpc.WithInsecure(), sgrpc.WithShardRedirect())
		if err != nil {
			panic(err)
		}
		userc = header.NewUserMgrClient(conn)
		eventc = header.NewEventMgrClient(conn)

		cluster := gocql.NewCluster("db-0")
		cluster.Timeout = 30 * time.Second
		cluster.Keyspace = "user"
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

func GetUser(accid, userid string) (*header.User, error) {
	waitUntilReady()
	// read in cache
	if value, found := cache.Get(accid + userid); found {
		if value == nil {
			return &header.User{AccountId: accid, Id: userid}, nil
		}
		return value.(*header.User), nil
	}

	user, err := userc.ReadUser(context.Background(), &header.Id{AccountId: accid, Id: userid})
	if err == nil {
		cache.SetWithTTL(accid+userid, user, 1000, 30*time.Second)
		return user, nil
	}

	u := &header.User{AccountId: accid, Id: userid}
	ub := make([]byte, 0)
	created, _ := idgen.GetCreated(userid, idgen.USER_PREFIX)
	hour := clock.UnixHour(created)
	err = cqlsession.Query(`SELECT attrs FROM users WHERE account_id=? AND hour=? AND id=?`, accid, hour, userid).Scan(&ub)
	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		cache.SetWithTTL(accid+userid, u, 1000, 30*time.Second)
		return u, nil
	}

	if err != nil {
		return nil, header.E500(err, header.E_database_error, accid)
	}

	if err := proto.Unmarshal(ub, u); err != nil {
		return nil, header.E500(err, header.E_invalid_proto, accid, userid)
	}

	lastSessionId := GetTextAttr(u, "latest_session_id")
	session, err := GetUserSession(accid, userid, lastSessionId)
	if err != nil {
		return nil, err
	}
	u.Session = session

	cache.SetWithTTL(accid+userid, u, 1000, 30*time.Second)
	return u, nil
}

// Ref git.subiz.net/user/com/common.go
func GetTextAttr(u *header.User, key string) string {
	key = strings.ToLower(strings.TrimSpace(key))
	for _, a := range u.GetAttributes() {
		if !SameKey(key, a.GetKey()) {
			continue
		}

		return a.GetText()
	}

	return ""
}

func SameKey(k1, k2 string) bool {
	return strings.TrimSpace(strings.ToLower(k1)) == strings.TrimSpace(strings.ToLower(k2))
}

// Ref git.subiz.net/user/session/sessiondb.go
func GetUserSession(accountId, userId, sessionId string) (*header.UserSession, error) {
	var ip string
	var timezone string
	var platform string
	var language string
	var userAgent string
	var screenResolution string
	var gaClientId string
	var adsNetwork string
	// srcTracebs := make([][]byte, 0)
	var referrer string
	var dstReferrer string
	var source string
	var gaTrackingIds []string
	startEventb := make([]byte, 0)
	latestEventb := make([]byte, 0)
	campaignbs := make([][]byte, 0)
	var timeOnSite int64

	err := cqlsession.Query(
		`SELECT
			ip,
			timezone,
			platform,
			language,
			user_agent,
			screen_resolution,
			ga_client_id,
			ads_network,
			referrer,
			destination_referrer,
			source,
			campaigns,
			ga_tracking_ids,
			start_event,
			latest_event,
			time_on_site
		FROM user_session
		WHERE account_id=? AND user_id=? AND id=?`,
		accountId, userId, sessionId,
	).Scan(
		&ip,
		&timezone,
		&platform,
		&language,
		&userAgent,
		&screenResolution,
		&gaClientId,
		&adsNetwork,
		// &srcTracebs,
		&referrer,
		&dstReferrer,
		&source,
		&campaignbs,
		&gaTrackingIds,
		&startEventb,
		&latestEventb,
		&timeOnSite,
	)

	if err != nil && err.Error() == gocql.ErrNotFound.Error() {
		return nil, nil
	}
	if err != nil {
		return nil, header.E500(err, header.E_database_error, accountId, userId, sessionId)
	}

	var startEvent, latestEvent header.Event
	proto.Unmarshal(startEventb, &startEvent)
	proto.Unmarshal(latestEventb, &latestEvent)

	campaigns := make([]*header.SessionCampaign, 0)
	for _, campaignb := range campaignbs {
		campaign := &header.SessionCampaign{}
		proto.Unmarshal(campaignb, campaign)
		campaigns = append(campaigns, campaign)
	}

	// srcTraces := make([]*header.SourceTrace, 0)
	// for _, srcTraceb := range srcTracebs {
	// 	srcTrace := &header.SourceTrace{}
	// 	proto.Unmarshal(srcTraceb, srcTrace)
	// 	srcTraces = append(srcTraces, srcTrace)
	// }

	out := &header.UserSession{
		AccountId:        accountId,
		UserId:           userId,
		Id:               sessionId,
		Ip:               ip,
		Timezone:         timezone,
		Platform:         platform,
		Language:         language,
		UserAgent:        userAgent,
		ScreenResolution: screenResolution,
		GaClientId:       gaClientId,
		AdsNetwork:       adsNetwork,
		// SourceTraces:        srcTraces,
		Referrer:            referrer,
		DestinationReferrer: dstReferrer,
		Source:              source,
		Campaigns:           campaigns,
		GaTrackingIds:       gaTrackingIds,
		StartEvent:          &startEvent,
		LatestEvent:         &latestEvent,
		TimeOnSite:          timeOnSite,
	}

	return out, nil
}

func SetUser(ctx *cpb.Context, u *header.User) error {
	_, err := userc.UpdateUser(sgrpc.ToGrpcCtx(ctx), u)
	return err
}

func CreateEvent(ctx *cpb.Context, accid, userid string, ev *header.Event) error {
	_, err := eventc.CreateEvent(sgrpc.ToGrpcCtx(ctx), &header.UserEvent{
		AccountId: accid,
		UserId:    userid,
		Event:     ev,
	})
	return err
}

func ListMarkUserIds(unixHour int64, accid string, f func(string, string) bool) error {
	waitUntilReady()
	if accid != "" {
		iter := cqlsession.Query(`SELECT id FROM user.upsert_mark WHERE unix_hour=? AND objname=? AND account_id=?`, unixHour, "users", accid).Iter()
		var id string
		for iter.Scan(&id) {
			if !f(accid, id) {
				break
			}
		}
		return iter.Close()
	}

	// all accounts
	iter := cqlsession.Query(`SELECT account_id, id FROM user.upsert_mark WHERE unix_hour=? AND objname=?`, unixHour, "users").Iter()
	var accountId, id string
	for iter.Scan(&accountId, &id) {
		if !f(accountId, id) {
			break
		}
	}
	return iter.Close()
}
