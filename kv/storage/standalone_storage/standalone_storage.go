package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

  "github.com/Connor1996/badger"
  "github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		// engines.go
		db: engine_util.CreateDB("standalone", conf),
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
  // https://godoc.org/github.com/dgraph-io/badger#DB.NewTransaction
  // For read-only transactions, set update to false
  txn := s.db.NewTransaction(false)
	return &StandAloneStorageReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
  txn := s.db.NewTransaction(true)
  for _, m := range batch {
    // modify.go
    switch m.Data.(type) {
    case storage.Put:
        // util.go
        engine_util.PutCF(s.db, m.Cf(), m.Key(), m.Value())
    case storage.Delete:
        // util.go
        engine_util.DeleteCF(s.db, m.Cf(), m.Key())
    }
  }
  return txn.Commit()
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
  // util.go
  // https://godoc.org/github.com/dgraph-io/badger#Txn.Get
  val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
  return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
  // cf_iterator.go
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
  // https://godoc.org/github.com/dgraph-io/badger#DB.NewTransaction
	r.txn.Discard()
}