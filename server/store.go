package main

import (
	"bytes"
	"encoding/binary"

	"github.com/golang/protobuf/proto"

	"github.com/boltdb/bolt"
	"github.com/darshanmaiya/raft/protos"
)

var (
	logBucket = []byte("log")
	tipKey    = []byte("tip")
	lastIndex = []byte("idx")

	metaBucket = []byte("meta")
	idKey      = []byte("id")
	stateKey   = []byte("state")
	termKey    = []byte("term")
	voteKey    = []byte("vote")

	endian = binary.BigEndian
)

// MetaStore...
type MetaStore struct {
	db *bolt.DB
}

func NewMetaStore(db *bolt.DB) (*MetaStore, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(metaBucket)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &MetaStore{db}, nil
}

func (m *MetaStore) UpdateID(newId uint32) error {
	var scratch [4]byte
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)
		endian.PutUint32(scratch[:], newId)
		return b.Put(idKey, scratch[:])
	})
}

func (m *MetaStore) FetchID() (uint32, error) {
	var id uint32
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)
		idBytes := b.Get(idKey)

		id = endian.Uint32(idBytes)
		return nil
	})
	if err != nil {
		return 0, err
	}

	return id, nil
}

func (m *MetaStore) UpdateState(newState NodeState) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)
		return b.Put(stateKey, []byte{byte(newState)})
	})
}

func (m *MetaStore) FetchState() (NodeState, error) {
	var state NodeState
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)
		stateByte := b.Get(stateKey)
		if stateByte == nil {
			state = Follower
		} else {
			state = NodeState(stateByte[0])
		}

		return nil
	})
	if err != nil {
		return 0, nil
	}

	return state, nil
}

func (m *MetaStore) UpdateCurrentTerm(newTerm uint32) error {
	return m.db.Update(func(tx *bolt.Tx) error {
		scratch := make([]byte, 4)

		b := tx.Bucket(metaBucket)
		endian.PutUint32(scratch, newTerm)
		return b.Put(termKey, scratch)
	})
}

func (m *MetaStore) IncrementTerm() error {
	var newTerm uint32

	return m.db.Update(func(tx *bolt.Tx) error {
		scratch := make([]byte, 4)

		b := tx.Bucket(metaBucket)
		termBytes := b.Get(termKey)

		if termBytes == nil {
			newTerm = 0
		} else {
			oldTerm := endian.Uint32(termBytes)
			newTerm = oldTerm + 1
		}

		endian.PutUint32(scratch, newTerm)

		return b.Put(termKey, scratch)
	})
}

func (m *MetaStore) FetchCurrentTerm() (uint32, error) {
	var term uint32
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)
		termBytes := b.Get(termKey)

		if termBytes == nil {
			term = 0
		} else {
			term = endian.Uint32(termBytes)
		}

		return nil
	})
	if err != nil {
		return 0, err
	}

	return term, nil
}

func (m *MetaStore) UpdateVotedFor(newTerm int32) error {
	var scratch [4]byte
	return m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)

		endian.PutUint32(scratch[:], uint32(newTerm))
		return b.Put(voteKey, scratch[:])
	})
}

func (m *MetaStore) FetchVotedFor() (int32, error) {
	votedFor := int32(-1)
	err := m.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(metaBucket)
		votedForBytes := b.Get(voteKey)
		if votedForBytes == nil {
			return nil
		}

		votedFor = int32(endian.Uint32(votedForBytes))
		return nil
	})
	if err != nil {
		return votedFor, err
	}

	return votedFor, nil
}

// LogStore...
type LogStore struct {
	db *bolt.DB
}

// NewLogStore...
func NewLogStore(db *bolt.DB) (*LogStore, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(logBucket)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &LogStore{db}, nil
}

func (l *LogStore) FetchTip() (*raft.LogEntry, error) {
	var tip *raft.LogEntry

	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		tipBytes := b.Get(lastIndex)
		protoBytes := b.Get(tipBytes)
		return proto.Unmarshal(protoBytes, tip)
	})
	if err != nil {
		return nil, err
	}

	return tip, err
}

// FetchEntry...
func (l *LogStore) FetchEntry(index uint32) (*raft.LogEntry, error) {
	var entry *raft.LogEntry

	var bytesIndex [4]byte
	endian.PutUint32(bytesIndex[:], index)

	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		bytes := b.Get(bytesIndex[:])
		if bytes == nil {
			return nil
		}
		entry = &raft.LogEntry{}

		return proto.Unmarshal(bytes, entry)
	})
	if err != nil {
		return nil, err
	}

	return entry, nil
}

// AddEntry...
func (l *LogStore) AddEntry(index uint32, entry *raft.LogEntry) error {
	var scratch [4]byte
	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)

		bytes, err := proto.Marshal(entry)
		if err != nil {
			return err
		}

		endian.PutUint32(scratch[:], index)

		// Update the counter for the latest entry.
		if err := b.Put(lastIndex, scratch[:]); err != nil {
			return err
		}

		return b.Put(scratch[:], bytes)
	})
	if err != nil {
		return err
	}

	return nil
}

// RemoveEntry...
func (l *LogStore) RemoveEntry(index uint32) error {
	var bytesIndex [4]byte
	endian.PutUint32(bytesIndex[:], index)

	err := l.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		return b.Delete(bytesIndex[:])
	})
	if err != nil {
		return err
	}

	return err
}

func (l *LogStore) FetchAllEntries() ([]*raft.LogEntry, error) {
	var entries []*raft.LogEntry

	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)

		ferr := b.ForEach(func(k, v []byte) error {
			if bytes.Equal(lastIndex, k) {
				return nil
			}

			entry := &raft.LogEntry{}
			if err := proto.Unmarshal(v, entry); err != nil {
				return err
			}

			entries = append(entries, entry)
			return nil
		})

		return ferr
	})
	if err != nil {
		return nil, err
	}

	return entries, nil
}

func (l *LogStore) LogLength() (uint32, error) {
	var length uint32
	err := l.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(logBucket)
		tipBytes := b.Get(lastIndex)

		if tipBytes == nil {
			length = 0
		} else {
			length = endian.Uint32(tipBytes)
		}

		return nil
	})
	if err != nil {
		return 0, nil
	}

	return length, nil
}
