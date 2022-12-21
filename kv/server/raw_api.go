package server

import (
	"context"

	"github.com/dgraph-io/badger/v3"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := &kvrpcpb.RawGetResponse{}
	s := server.storage

	r, e := s.Reader(req.Context)
	if e != nil {
		resp.Error = e.Error()
		return resp, e
	}
	defer r.Close()

	v, e := r.GetCF(req.Cf, req.Key)
	if e != nil {
		if e == badger.ErrKeyNotFound {
			resp.NotFound = true
			return resp, nil
		}
		resp.Error = e.Error()
		return resp, e
	}

	if v == nil {
		resp.NotFound = true
	}

	resp.Value = v
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := &kvrpcpb.RawPutResponse{}

	s := server.storage

	p := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}

	m := storage.Modify{Data: p}

	err := s.Write(req.Context, []storage.Modify{m})
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}

	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := &kvrpcpb.RawDeleteResponse{}

	s := server.storage

	d := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	m := storage.Modify{Data: d}

	err := s.Write(req.Context, []storage.Modify{m})
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}

	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := &kvrpcpb.RawScanResponse{}

	s := server.storage

	r, e := s.Reader(req.Context)
	if e != nil {
		resp.Error = e.Error()
		return resp, e
	}
	defer r.Close()

	it := r.IterCF(req.Cf)
	defer it.Close()

	var i uint32 = 0
	for it.Seek(req.StartKey); it.Valid(); it.Next() {
		if i == req.Limit {
			break
		}
		i += 1
		k := it.Item().Key()
		v, e := it.Item().Value()
		if e != nil {
			continue
		}
		kv := kvrpcpb.KvPair{
			Key:   k,
			Value: v,
		}
		resp.Kvs = append(resp.Kvs, &kv)
	}

	return resp, nil
}
