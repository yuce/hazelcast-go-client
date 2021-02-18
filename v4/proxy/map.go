package proxy

import (
	"github.com/hazelcast/hazelcast-go-client/v3/core"
	"time"
)

type mapProxy struct {
}

func (m mapProxy) Destroy() (bool, error) {
	panic("implement me")
}

func (m mapProxy) Name() string {
	panic("implement me")
}

func (m mapProxy) PartitionKey() string {
	panic("implement me")
}

func (m mapProxy) ServiceName() string {
	panic("implement me")
}

func (m mapProxy) Put(key interface{}, value interface{}) (oldValue interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) Get(key interface{}) (value interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) Remove(key interface{}) (value interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) RemoveIfSame(key interface{}, value interface{}) (ok bool, err error) {
	panic("implement me")
}

func (m mapProxy) RemoveAll(predicate interface{}) (err error) {
	panic("implement me")
}

func (m mapProxy) Size() (size int32, err error) {
	panic("implement me")
}

func (m mapProxy) Aggregate(aggregator interface{}) (result interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) AggregateWithPredicate(aggregator interface{}, predicate interface{}) (result interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) ContainsKey(key interface{}) (found bool, err error) {
	panic("implement me")
}

func (m mapProxy) ContainsValue(value interface{}) (found bool, err error) {
	panic("implement me")
}

func (m mapProxy) Clear() (err error) {
	panic("implement me")
}

func (m mapProxy) Delete(key interface{}) (err error) {
	panic("implement me")
}

func (m mapProxy) IsEmpty() (empty bool, err error) {
	panic("implement me")
}

func (m mapProxy) AddIndex(attribute string, ordered bool) (err error) {
	panic("implement me")
}

func (m mapProxy) Evict(key interface{}) (evicted bool, err error) {
	panic("implement me")
}

func (m mapProxy) EvictAll() (err error) {
	panic("implement me")
}

func (m mapProxy) Flush() (err error) {
	panic("implement me")
}

func (m mapProxy) ForceUnlock(key interface{}) (err error) {
	panic("implement me")
}

func (m mapProxy) Lock(key interface{}) (err error) {
	panic("implement me")
}

func (m mapProxy) LockWithLeaseTime(key interface{}, lease time.Duration) (err error) {
	panic("implement me")
}

func (m mapProxy) Unlock(key interface{}) (err error) {
	panic("implement me")
}

func (m mapProxy) IsLocked(key interface{}) (locked bool, err error) {
	panic("implement me")
}

func (m mapProxy) Replace(key interface{}, value interface{}) (oldValue interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) ReplaceIfSame(key interface{}, oldValue interface{}, newValue interface{}) (replaced bool, err error) {
	panic("implement me")
}

func (m mapProxy) Set(key interface{}, value interface{}) (err error) {
	panic("implement me")
}

func (m mapProxy) SetWithTTL(key interface{}, value interface{}, ttl time.Duration) (err error) {
	panic("implement me")
}

func (m mapProxy) PutIfAbsent(key interface{}, value interface{}) (oldValue interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) PutAll(entries map[interface{}]interface{}) (err error) {
	panic("implement me")
}

func (m mapProxy) Project(projection interface{}) (result []interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) ProjectWithPredicate(projection interface{}, predicate interface{}) (result []interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) KeySet() (keySet []interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) KeySetWithPredicate(predicate interface{}) (keySet []interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) Values() (values []interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) ValuesWithPredicate(predicate interface{}) (values []interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) EntrySet() (resultPairs []core.Pair, err error) {
	panic("implement me")
}

func (m mapProxy) EntrySetWithPredicate(predicate interface{}) (resultPairs []core.Pair, err error) {
	panic("implement me")
}

func (m mapProxy) TryLock(key interface{}) (locked bool, err error) {
	panic("implement me")
}

func (m mapProxy) TryLockWithTimeout(key interface{}, timeout time.Duration) (locked bool, err error) {
	panic("implement me")
}

func (m mapProxy) TryLockWithTimeoutAndLease(key interface{}, timeout time.Duration, lease time.Duration) (locked bool, err error) {
	panic("implement me")
}

func (m mapProxy) TryPut(key interface{}, value interface{}) (ok bool, err error) {
	panic("implement me")
}

func (m mapProxy) TryPutWithTimeout(key interface{}, value interface{}, timeout time.Duration) (ok bool, err error) {
	panic("implement me")
}

func (m mapProxy) TryRemove(key interface{}, timeout time.Duration) (ok bool, err error) {
	panic("implement me")
}

func (m mapProxy) GetAll(keys []interface{}) (entryMap map[interface{}]interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) GetEntryView(key interface{}) (entryView core.EntryView, err error) {
	panic("implement me")
}

func (m mapProxy) PutTransient(key interface{}, value interface{}, ttl time.Duration) (err error) {
	panic("implement me")
}

func (m mapProxy) AddEntryListener(listener interface{}, includeValue bool) (registrationID string, err error) {
	panic("implement me")
}

func (m mapProxy) AddEntryListenerWithPredicate(listener interface{}, predicate interface{}, includeValue bool) (registrationID string, err error) {
	panic("implement me")
}

func (m mapProxy) AddEntryListenerToKey(listener interface{}, key interface{}, includeValue bool) (registrationID string, err error) {
	panic("implement me")
}

func (m mapProxy) AddEntryListenerToKeyWithPredicate(listener interface{}, predicate interface{}, key interface{}, includeValue bool) (registrationID string, err error) {
	panic("implement me")
}

func (m mapProxy) RemoveEntryListener(registrationID string) (removed bool, err error) {
	panic("implement me")
}

func (m mapProxy) ExecuteOnKey(key interface{}, entryProcessor interface{}) (result interface{}, err error) {
	panic("implement me")
}

func (m mapProxy) ExecuteOnKeys(keys []interface{}, entryProcessor interface{}) (keyToResultPairs []core.Pair, err error) {
	panic("implement me")
}

func (m mapProxy) ExecuteOnEntries(entryProcessor interface{}) (keyToResultPairs []core.Pair, err error) {
	panic("implement me")
}

func (m mapProxy) ExecuteOnEntriesWithPredicate(entryProcessor interface{}, predicate interface{}) (keyToResultPairs []core.Pair, err error) {
	panic("implement me")
}
