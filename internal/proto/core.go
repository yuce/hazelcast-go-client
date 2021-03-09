// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proto

import (
	"bytes"
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"reflect"
	"time"

	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
	"github.com/hazelcast/hazelcast-go-client/v4/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/util/timeutil"
)

type uuid struct {
	msb int64
	lsb int64
}

type Member struct {
	address      *hazelcast.Address
	uuid         hazelcast.UUID
	isLiteMember bool
	attributes   map[string]string
	version      MemberVersion
	addressMap   map[EndpointQualifier]*hazelcast.Address
}

func NewMember(address *hazelcast.Address, uuid hazelcast.UUID, isLiteMember bool, attributes map[string]string, version MemberVersion, addressMap map[EndpointQualifier]*hazelcast.Address) *Member {
	return &Member{address: address, uuid: uuid, isLiteMember: isLiteMember, attributes: attributes, version: version, addressMap: addressMap}
}

func (m Member) Address() *hazelcast.Address {
	return m.address
}

func (m Member) UUID() hazelcast.UUID {
	return m.uuid
}

func (m Member) LiteMember() bool {
	return m.isLiteMember
}

func (m Member) Attributes() map[string]string {
	return m.attributes
}

func (m *Member) String() string {
	memberInfo := fmt.Sprintf("Member %s - %s", m.address.String(), m.UUID())
	if m.LiteMember() {
		memberInfo += " lite"
	}
	return memberInfo
}

func (m *Member) HasSameAddress(member *Member) bool {
	return m.address == member.address
}

func (p *Pair) Key() interface{} {
	return p.key
}

func (p *Pair) Value() interface{} {
	return p.value
}

func (m *Member) Equal(member2 Member) bool {
	if m.address != member2.address {
		return false
	}
	if m.uuid != member2.uuid {
		return false
	}
	if m.isLiteMember != member2.isLiteMember {
		return false
	}
	if !reflect.DeepEqual(m.attributes, member2.attributes) {
		return false
	}
	return true
}

type MemberAttributeEvent struct {
	operationType int32
	key           string
	value         string
	member        hazelcast.Member
}

func NewMemberAttributeEvent(operationType int32, key string, value string, member hazelcast.Member) *MemberAttributeEvent {
	return &MemberAttributeEvent{
		operationType: operationType,
		key:           key,
		value:         value,
		member:        member,
	}
}

func (m *MemberAttributeEvent) OperationType() int32 {
	return m.operationType
}

func (m *MemberAttributeEvent) Key() string {
	return m.key
}

func (m *MemberAttributeEvent) Value() string {
	return m.value
}

func (m *MemberAttributeEvent) Member() hazelcast.Member {
	return m.member
}

type DistributedObjectInfo struct {
	name        string
	serviceName string
}

func (i *DistributedObjectInfo) Name() string {
	return i.name
}

func (i *DistributedObjectInfo) ServiceName() string {
	return i.serviceName
}

func (i *DistributedObjectInfo) GetName() string {
	return i.name
}

func (i *DistributedObjectInfo) GetServiceName() string {
	return i.serviceName
}

func NewDistributedObjectInfo(name string, serviceName string) DistributedObjectInfo {
	return DistributedObjectInfo{name: name, serviceName: serviceName}
}

type DataEntryView struct {
	keyData                serialization.Data
	valueData              serialization.Data
	cost                   int64
	creationTime           int64
	expirationTime         int64
	hits                   int64
	lastAccessTime         int64
	lastStoredTime         int64
	lastUpdateTime         int64
	version                int64
	evictionCriteriaNumber int64
	ttl                    int64
}

func (ev *DataEntryView) KeyData() serialization.Data {
	return ev.keyData
}

func (ev *DataEntryView) ValueData() serialization.Data {
	return ev.valueData
}

func (ev *DataEntryView) Cost() int64 {
	return ev.cost
}

func (ev *DataEntryView) CreationTime() int64 {
	return ev.creationTime
}

func (ev *DataEntryView) ExpirationTime() int64 {
	return ev.expirationTime
}

func (ev *DataEntryView) Hits() int64 {
	return ev.hits
}

func (ev *DataEntryView) LastAccessTime() int64 {
	return ev.lastAccessTime
}

func (ev *DataEntryView) LastStoredTime() int64 {
	return ev.lastStoredTime
}

func (ev *DataEntryView) LastUpdateTime() int64 {
	return ev.lastUpdateTime
}

func (ev *DataEntryView) Version() int64 {
	return ev.version
}

func (ev *DataEntryView) EvictionCriteriaNumber() int64 {
	return ev.evictionCriteriaNumber
}

func (ev *DataEntryView) TTL() int64 {
	return ev.ttl
}

type EntryView struct {
	key                    interface{}
	value                  interface{}
	cost                   int64
	creationTime           time.Time
	expirationTime         time.Time
	hits                   int64
	lastAccessTime         time.Time
	lastStoredTime         time.Time
	lastUpdateTime         time.Time
	version                int64
	evictionCriteriaNumber int64
	ttl                    time.Duration
}

func NewEntryView(key interface{}, value interface{}, cost int64, creationTime int64, expirationTime int64, hits int64,
	lastAccessTime int64, lastStoredTime int64, lastUpdateTime int64, version int64, evictionCriteriaNumber int64, ttl int64) *EntryView {
	return &EntryView{
		key:                    key,
		value:                  value,
		cost:                   cost,
		creationTime:           timeutil.ConvertMillisToUnixTime(creationTime),
		expirationTime:         timeutil.ConvertMillisToUnixTime(expirationTime),
		hits:                   hits,
		lastAccessTime:         timeutil.ConvertMillisToUnixTime(lastAccessTime),
		lastStoredTime:         timeutil.ConvertMillisToUnixTime(lastStoredTime),
		lastUpdateTime:         timeutil.ConvertMillisToUnixTime(lastUpdateTime),
		version:                version,
		evictionCriteriaNumber: evictionCriteriaNumber,
		ttl:                    timeutil.ConvertMillisToDuration(ttl),
	}
}

func (ev *EntryView) Key() interface{} {
	return ev.key
}

func (ev *EntryView) Value() interface{} {
	return ev.value
}

func (ev *EntryView) Cost() int64 {
	return ev.cost
}

func (ev *EntryView) CreationTime() time.Time {
	return ev.creationTime
}

func (ev *EntryView) ExpirationTime() time.Time {
	return ev.expirationTime
}

func (ev *EntryView) Hits() int64 {
	return ev.hits
}

func (ev *EntryView) LastAccessTime() time.Time {
	return ev.lastAccessTime
}

func (ev *EntryView) LastStoredTime() time.Time {
	return ev.lastStoredTime
}

func (ev *EntryView) LastUpdateTime() time.Time {
	return ev.lastUpdateTime
}

func (ev *EntryView) Version() int64 {
	return ev.version
}

func (ev *EntryView) EvictionCriteriaNumber() int64 {
	return ev.evictionCriteriaNumber
}

func (ev *EntryView) TTL() time.Duration {
	return ev.ttl
}

func (ev DataEntryView) Equal(ev2 DataEntryView) bool {
	if !bytes.Equal(ev.keyData.Buffer(), ev2.keyData.Buffer()) || !bytes.Equal(ev.valueData.Buffer(), ev2.valueData.Buffer()) {
		return false
	}
	if ev.cost != ev2.cost || ev.creationTime != ev2.creationTime || ev.expirationTime != ev2.expirationTime || ev.hits != ev2.hits {
		return false
	}
	if ev.lastAccessTime != ev2.lastAccessTime || ev.lastStoredTime != ev2.lastStoredTime || ev.lastUpdateTime != ev2.lastUpdateTime {
		return false
	}
	if ev.version != ev2.version || ev.evictionCriteriaNumber != ev2.evictionCriteriaNumber || ev.ttl != ev2.ttl {
		return false
	}
	return true
}

type ServerError struct {
	errorCode      int32
	className      string
	message        string
	stackTrace     []hazelcast.StackTraceElement
	causeErrorCode int32
	causeClassName string
}

func (e *ServerError) Error() string {
	return e.message
}

func (e *ServerError) ErrorCode() int32 {
	return e.errorCode
}

func (e *ServerError) ClassName() string {
	return e.className
}

func (e *ServerError) Message() string {
	return e.message
}

func (e *ServerError) StackTrace() []hazelcast.StackTraceElement {
	stackTrace := make([]hazelcast.StackTraceElement, len(e.stackTrace))
	for i, v := range e.stackTrace {
		stackTrace[i] = v
	}
	return stackTrace
}

func (e *ServerError) CauseErrorCode() int32 {
	return e.causeErrorCode
}

func (e *ServerError) CauseClassName() string {
	return e.causeClassName
}

type AbstractMapEvent struct {
	name      string
	member    hazelcast.Member
	eventType int32
}

func (e *AbstractMapEvent) Name() string {
	return e.name
}

func (e *AbstractMapEvent) Member() hazelcast.Member {
	return e.member
}

func (e *AbstractMapEvent) EventType() int32 {
	return e.eventType
}

func (e *AbstractMapEvent) String() string {
	return fmt.Sprintf("entryEventType = %d, member = %v, name = '%s'",
		e.eventType, e.member, e.name)
}

type EntryEvent struct {
	*AbstractMapEvent
	key          interface{}
	value        interface{}
	oldValue     interface{}
	mergingValue interface{}
}

func NewEntryEvent(name string, member hazelcast.Member, eventType int32, key interface{}, value interface{},
	oldValue interface{}, mergingValue interface{}) *EntryEvent {
	return &EntryEvent{
		AbstractMapEvent: &AbstractMapEvent{name, member, eventType},
		key:              key,
		value:            value,
		oldValue:         oldValue,
		mergingValue:     mergingValue,
	}
}

func (e *EntryEvent) Key() interface{} {
	return e.key
}

func (e *EntryEvent) Value() interface{} {
	return e.value
}

func (e *EntryEvent) OldValue() interface{} {
	return e.oldValue
}

func (e *EntryEvent) MergingValue() interface{} {
	return e.mergingValue
}

type MapEvent struct {
	*AbstractMapEvent
	numberOfAffectedEntries int32
}

func NewMapEvent(name string, member hazelcast.Member, eventType int32, numberOfAffectedEntries int32) hazelcast.MapEvent {
	return &MapEvent{
		AbstractMapEvent:        &AbstractMapEvent{name, member, eventType},
		numberOfAffectedEntries: numberOfAffectedEntries,
	}
}

func (e *MapEvent) NumberOfAffectedEntries() int32 {
	return e.numberOfAffectedEntries
}

func (e *MapEvent) String() string {
	return fmt.Sprintf("MapEvent{%s, numberOfAffectedEntries = %d}", e.AbstractMapEvent.String(), e.numberOfAffectedEntries)
}

type ItemEvent struct {
	name      string
	item      interface{}
	eventType int32
	member    hazelcast.Member
}

func NewItemEvent(name string, item interface{}, eventType int32, member hazelcast.Member) hazelcast.ItemEvent {
	return &ItemEvent{
		name:      name,
		item:      item,
		eventType: eventType,
		member:    member,
	}
}

func (e *ItemEvent) Name() string {
	return e.name
}

func (e *ItemEvent) Item() interface{} {
	return e.item
}

func (e *ItemEvent) EventType() int32 {
	return e.eventType
}

func (e *ItemEvent) Member() hazelcast.Member {
	return e.member
}

type DecodeListenerResponse func(message *ClientMessage) string
type EncodeListenerRemoveRequest func(registrationID string) *ClientMessage

// Helper function to get flags for listeners
func GetMapListenerFlags(listener interface{}) (int32, error) {
	flags := int32(0)
	if _, ok := listener.(hazelcast.EntryAddedListener); ok {
		flags |= bufutil.EntryEventAdded
	}
	if _, ok := listener.(hazelcast.EntryLoadedListener); ok {
		flags |= bufutil.EntryEventLoaded
	}
	if _, ok := listener.(hazelcast.EntryRemovedListener); ok {
		flags |= bufutil.EntryEventRemoved
	}
	if _, ok := listener.(hazelcast.EntryUpdatedListener); ok {
		flags |= bufutil.EntryEventUpdated
	}
	if _, ok := listener.(hazelcast.EntryEvictedListener); ok {
		flags |= bufutil.EntryEventEvicted
	}
	if _, ok := listener.(hazelcast.MapEvictedListener); ok {
		flags |= bufutil.MapEventEvicted
	}
	if _, ok := listener.(hazelcast.MapClearedListener); ok {
		flags |= bufutil.MapEventCleared
	}
	if _, ok := listener.(hazelcast.EntryExpiredListener); ok {
		flags |= bufutil.EntryEventExpired
	}
	if _, ok := listener.(hazelcast.EntryMergedListener); ok {
		flags |= bufutil.EntryEventMerged
	}
	if flags == 0 {
		return 0, hazelcast.NewHazelcastIllegalArgumentError(fmt.Sprintf("not a supported listener type: %v",
			reflect.TypeOf(listener)), nil)
	}
	return flags, nil
}

type TopicMessage struct {
	messageObject    interface{}
	publishTime      time.Time
	publishingMember hazelcast.Member
}

func NewTopicMessage(messageObject interface{}, publishTime int64, publishingMember hazelcast.Member) *TopicMessage {
	return &TopicMessage{
		messageObject:    messageObject,
		publishTime:      timeutil.ConvertMillisToUnixTime(publishTime),
		publishingMember: publishingMember,
	}
}

func (m *TopicMessage) MessageObject() interface{} {
	return m.messageObject
}

func (m *TopicMessage) PublishTime() time.Time {
	return m.publishTime
}

func (m *TopicMessage) PublishingMember() hazelcast.Member {
	return m.publishingMember
}

// MemberVersion
type MemberVersion struct {
	major int8
	minor int8
	patch int8
}

func NewMemberVersion(major, minor, patch byte) MemberVersion {
	return MemberVersion{int8(major), int8(minor), int8(patch)}
}

func (memberVersion MemberVersion) Major() byte {
	return byte(memberVersion.major)
}

func (memberVersion MemberVersion) Minor() byte {
	return byte(memberVersion.minor)
}

func (memberVersion MemberVersion) Patch() byte {
	return byte(memberVersion.patch)
}

// MemberInfo represents a member in the cluster with its address, uuid, lite member status, attributes and version.
type MemberInfo struct {
	// address is proto.Address: Address of the member.
	address *hazelcast.Address

	// uuid is hazelcast.UUID: UUID of the member.
	uuid hazelcast.UUID

	// liteMember represents member is a lite member. Lite members do not own any partition.
	liteMember bool

	// attributes are configured attributes of the member
	attributes map[string]string

	// version is hazelcast.MemberVersion: Hazelcast codebase version of the member.
	version MemberVersion

	// addressMap
	addressMap map[EndpointQualifier]*hazelcast.Address
}

func NewMemberInfo(address *hazelcast.Address, uuid hazelcast.UUID, attributes map[string]string, liteMember bool, version MemberVersion,
	isAddressMapExists bool, addressMap interface{}) MemberInfo {
	// TODO: Convert addressMap to map[EndpointQualifier]*Address
	// copy address
	addressCopy := *address
	return MemberInfo{address: &addressCopy, uuid: uuid, attributes: attributes, liteMember: liteMember, version: version,
		addressMap: addressMap.(map[EndpointQualifier]*hazelcast.Address)}
}

func (memberInfo MemberInfo) Address() *hazelcast.Address {
	return memberInfo.address
}

func (memberInfo MemberInfo) Uuid() hazelcast.UUID {
	return memberInfo.uuid
}

func (memberInfo MemberInfo) Attributes() map[string]string {
	return memberInfo.attributes
}

func (memberInfo MemberInfo) LiteMember() bool {
	return memberInfo.liteMember
}

func (memberInfo MemberInfo) Version() MemberVersion {
	return memberInfo.version
}

func (memberInfo MemberInfo) AddressMap() map[EndpointQualifier]*hazelcast.Address {
	return memberInfo.addressMap
}

type EndpointQualifier struct {
	_type      int32
	identifier string
}

func NewEndpointQualifier(_type int32, identifier string) EndpointQualifier {
	return EndpointQualifier{_type, identifier}
}

func (e EndpointQualifier) Type() int32 {
	return e._type
}

func (e EndpointQualifier) Identifier() string {
	return e.identifier
}
