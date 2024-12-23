package storage

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
)

type PersistentStorageServer interface {
	Engine() queueEngine
	HandlerTorrent() error
	Close() error
}

type sqlDatabase interface {
	Engine() sqlDatabaseEngine
	DoesTorrentExist(infoHash []byte) (bool, error)
	AddNewTorrent(infoHash []byte, name string, files []File) error
	Close() error   // Exit the SQL Service link after the security is closed
	IsClosed() bool // Listens for abnormalities in the service and automatically reconnects when the service is abnormal

	// GetNumberOfTorrents returns the number of torrents saved in the database. Might be an
	// approximation.
	GetNumberOfTorrents() (uint, error)
	// QueryTorrents returns @pageSize amount of torrents,
	// * that are discovered before @discoveredOnBefore
	// * that match the @query if it's not empty, else all torrents
	// * ordered by the @orderBy in ascending order if @ascending is true, else in descending order
	// after skipping (@page * @pageSize) torrents that also fits the criteria above.
	//
	// On error, returns (nil, error), otherwise a non-nil slice of TorrentMetadata and nil.
	QueryTorrents(
		query string,
		epoch int64,
		orderBy OrderingCriteria,
		ascending bool,
		limit uint64,
		lastOrderedValue *float64,
		lastID *uint64,
	) ([]TorrentMetadata, error)
	// GetTorrents returns the TorrentExtMetadata for the torrent of the given InfoHash. Will return
	// nil, nil if the torrent does not exist in the database.
	GetTorrent(infoHash []byte) (*TorrentMetadata, error)
	GetFiles(infoHash []byte) ([]File, error)
	GetStatistics(from string, n uint) (*Statistics, error)
}

type OrderingCriteria uint8

const (
	ByRelevance OrderingCriteria = iota
	ByTotalSize
	ByDiscoveredOn
	ByNFiles
	ByNSeeders
	ByNLeechers
	ByUpdatedOn
)

// TODO: search `swtich (orderBy)` and see if all cases are covered all the time

type sqlDatabaseEngine uint8
type queueEngine sqlDatabaseEngine

const (
	Postgres sqlDatabaseEngine = iota + 1
)

const (
	RabbitMQ queueEngine = iota + 1
)

// An error that occurred when the queue read or wrote data to the database.
// These error types are used for control over the message.
var (
	InfoHashExistErr    = errors.New("InfoHashExistSQL")
	DoesTorrentExistErr = errors.New("DoesTorrentExistErr")

	SqlTransactionBeginErr  = errors.New("SqlTransactionBeginErr")
	SqlTransactionCommitErr = errors.New("SqlTransactionCommitErr")

	InsertTorrentsErr      = errors.New("InsertTorrentsErr")
	InsertTorrentsFilesErr = errors.New("InsertTorrentsFilesErr")

	SqlTransactionRateLimiting = errors.New("SQLTransactionRateLimiting")
)

// SqlErr use in queues to enhance the management of messages in queues
// Msg Customized messages.
// ErrType Write the error types defined above, and add them if there is no suitable one.
// Err Normal err.
type SqlErr struct {
	Msg          string
	ErrType, Err error
}

func (s SqlErr) Error() string {
	if s.Err != nil && s.ErrType != nil {
		return fmt.Sprintf("%v:%s\n%v", s.ErrType, s.Msg, s.Err)
	}
	return s.Msg
}

// newSqlErr use in queues to enhance the management of messages in queues
func newSqlErr(message string, errType, err error) error {
	return &SqlErr{
		Msg:     message,
		Err:     err,
		ErrType: errType,
	}
}

type Statistics struct {
	NDiscovered map[string]uint64 `json:"nDiscovered"`
	NFiles      map[string]uint64 `json:"nFiles"`
	TotalSize   map[string]uint64 `json:"totalSize"`
}

type File struct {
	Size int64  `json:"size"`
	Path string `json:"path"`
}

type TorrentMetadata struct {
	ID           uint64  `json:"id"`
	InfoHash     []byte  `json:"infoHash"` // marshalled differently
	Name         string  `json:"name"`
	Size         uint64  `json:"size"`
	DiscoveredOn int64   `json:"discoveredOn"`
	NFiles       uint    `json:"nFiles"`
	Relevance    float64 `json:"relevance"`
}

type SimpleTorrentSummary struct {
	InfoHash string `json:"infoHash"`
	Name     string `json:"name"`
	Files    []File `json:"files"`
}

func (tm *TorrentMetadata) MarshalJSON() ([]byte, error) {
	type Alias TorrentMetadata
	return json.Marshal(&struct {
		InfoHash string `json:"infoHash"`
		*Alias
	}{
		InfoHash: hex.EncodeToString(tm.InfoHash),
		Alias:    (*Alias)(tm),
	})
}

func MakePersistentStorageServer(rawQueueURL, rawSqlURL string) (PersistentStorageServer, error) {
	queueUrl_, err := url.Parse(rawQueueURL)
	if err != nil {
		return nil, errors.New("url.Parse: MQ URL: " + err.Error())
	}
	sqlUrl_, err := url.Parse(rawSqlURL)
	if err != nil {
		return nil, errors.New("url.Parse: SQL URL: " + err.Error())
	}

	switch queueUrl_.Scheme {

	case "amqp", "amqps":
		return makeRabbitMQ(queueUrl_, sqlUrl_)

	default:
		return nil, fmt.Errorf("unknown MQ URI scheme: `%s`", queueUrl_.Scheme)
	}
}

func NewStatistics() (s *Statistics) {
	s = new(Statistics)
	s.NDiscovered = make(map[string]uint64)
	s.NFiles = make(map[string]uint64)
	s.TotalSize = make(map[string]uint64)
	return
}

// sqlDB : It is used in queue GO code to link SQL services.
type sqlDB struct {
	sqlUrl *url.URL
	sqlDatabase
}

// connectSqlDB : It is used in queue GO code to link SQL services.
func (s *sqlDB) connectSqlDB() (err error) {
	switch s.sqlUrl.Scheme {
	case "postgres", "cockroach":
		s.sqlDatabase, err = newPostgresDB(s.sqlUrl)
		if err != nil {
			return err
		}
		return nil
	default:
		return errors.New("unknown SQL DB")
	}
}
