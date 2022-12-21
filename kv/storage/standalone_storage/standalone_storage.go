package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	badger "github.com/dgraph-io/badger/v3"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf *config.Config
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	db, err := badger.Open(badger.DefaultOptions(s.conf.DBPath))
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	if s.db == nil {
		return nil
	}
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return &badgerRead{txn: s.db.NewTransaction(false)}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	errCh := make(chan error, 1)

	for _, m := range batch {
		prefix := []byte(m.Cf())

		key := make([]byte, 0, len(prefix)+len(m.Key()))
		key = append(key, prefix...)
		key = append(key, m.Key()...)

		err := txn.Set(key, m.Value())

		if err != nil {
			errCh <- err
			break
		}
	}

	select {
	case e := <-errCh:
		return e
	default:
		if err := txn.Commit(); err != nil {
			return err
		}
		return nil
	}
}

type badgerRead struct {
	txn *badger.Txn
}

func (r *badgerRead) GetCF(cf string, key []byte) ([]byte, error) {

	k := make([]byte, 0, len(cf)+len(key))
	k = append(k, []byte(cf)...)
	k = append(k, key...)
	item, err := r.txn.Get(k)
	if err != nil {
		return nil, err
	}

	if item.IsDeletedOrExpired() {
		return nil, nil
	}

	v, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	return v, nil
}
func (r *badgerRead) IterCF(cf string) engine_util.DBIterator {
	it := r.txn.NewIterator(badger.DefaultIteratorOptions)
	return &badgerDBIterate{it: it, cf: cf}
}
func (r *badgerRead) Close() {
	r.txn.Discard()
}

type badgerDBIterate struct {
	it *badger.Iterator
	cf string
}

var _ engine_util.DBIterator = (*badgerDBIterate)(nil)

func (iter *badgerDBIterate) Item() engine_util.DBItem {
	return &item{i: iter.it.Item(), cf: iter.cf}
}

func (iter *badgerDBIterate) Valid() bool {
	return iter.it.Valid()
}

func (iter *badgerDBIterate) Next() {
	if iter.Valid() {
		iter.it.Next()
		for iter.Valid() && iter.Item().ValueSize() == 0 {
			iter.it.Next()
		}
	}
}

func (iter *badgerDBIterate) Seek(key []byte) {
	k := make([]byte, 0, len(iter.cf)+len(key))
	k = append(k, []byte(iter.cf)...)
	k = append(k, key...)
	iter.it.Seek(k)
}

func (iter *badgerDBIterate) Close() { iter.it.Close() }

type item struct {
	i  *badger.Item
	cf string
}

func (i *item) Key() []byte { return i.KeyCopy(nil) }

func (i *item) KeyCopy(dst []byte) []byte {
	k := i.i.KeyCopy(dst)
	return k[len([]byte(i.cf)):]
}

func (i *item) Value() ([]byte, error) { return i.ValueCopy(nil) }

func (i *item) ValueCopy(dst []byte) ([]byte, error) { return i.i.ValueCopy(nil) }

func (i *item) ValueSize() int { return int(i.i.ValueSize()) }
