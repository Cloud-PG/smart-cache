// Code generated by protoc-gen-go. DO NOT EDIT.
// source: simService/simService.proto

package simservice

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	empty "github.com/golang/protobuf/ptypes/empty"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type ActionResult struct {
	Filename             string   `protobuf:"bytes,1,opt,name=filename,proto3" json:"filename,omitempty"`
	Added                bool     `protobuf:"varint,2,opt,name=added,proto3" json:"added,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ActionResult) Reset()         { *m = ActionResult{} }
func (m *ActionResult) String() string { return proto.CompactTextString(m) }
func (*ActionResult) ProtoMessage()    {}
func (*ActionResult) Descriptor() ([]byte, []int) {
	return fileDescriptor_20cb65478f01afe7, []int{0}
}

func (m *ActionResult) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ActionResult.Unmarshal(m, b)
}
func (m *ActionResult) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ActionResult.Marshal(b, m, deterministic)
}
func (m *ActionResult) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ActionResult.Merge(m, src)
}
func (m *ActionResult) XXX_Size() int {
	return xxx_messageInfo_ActionResult.Size(m)
}
func (m *ActionResult) XXX_DiscardUnknown() {
	xxx_messageInfo_ActionResult.DiscardUnknown(m)
}

var xxx_messageInfo_ActionResult proto.InternalMessageInfo

func (m *ActionResult) GetFilename() string {
	if m != nil {
		return m.Filename
	}
	return ""
}

func (m *ActionResult) GetAdded() bool {
	if m != nil {
		return m.Added
	}
	return false
}

type SimCacheStatus struct {
	HitRate              float32  `protobuf:"fixed32,1,opt,name=hitRate,proto3" json:"hitRate,omitempty"`
	WeightedHitRate      float32  `protobuf:"fixed32,2,opt,name=weightedHitRate,proto3" json:"weightedHitRate,omitempty"`
	HitOverMiss          float32  `protobuf:"fixed32,3,opt,name=hitOverMiss,proto3" json:"hitOverMiss,omitempty"`
	Size                 float32  `protobuf:"fixed32,4,opt,name=size,proto3" json:"size,omitempty"`
	Capacity             float32  `protobuf:"fixed32,5,opt,name=capacity,proto3" json:"capacity,omitempty"`
	DataWritten          float32  `protobuf:"fixed32,6,opt,name=dataWritten,proto3" json:"dataWritten,omitempty"`
	DataRead             float32  `protobuf:"fixed32,7,opt,name=dataRead,proto3" json:"dataRead,omitempty"`
	DataReadOnHit        float32  `protobuf:"fixed32,8,opt,name=dataReadOnHit,proto3" json:"dataReadOnHit,omitempty"`
	DataReadOnMiss       float32  `protobuf:"fixed32,9,opt,name=dataReadOnMiss,proto3" json:"dataReadOnMiss,omitempty"`
	DataDeleted          float32  `protobuf:"fixed32,10,opt,name=dataDeleted,proto3" json:"dataDeleted,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SimCacheStatus) Reset()         { *m = SimCacheStatus{} }
func (m *SimCacheStatus) String() string { return proto.CompactTextString(m) }
func (*SimCacheStatus) ProtoMessage()    {}
func (*SimCacheStatus) Descriptor() ([]byte, []int) {
	return fileDescriptor_20cb65478f01afe7, []int{1}
}

func (m *SimCacheStatus) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SimCacheStatus.Unmarshal(m, b)
}
func (m *SimCacheStatus) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SimCacheStatus.Marshal(b, m, deterministic)
}
func (m *SimCacheStatus) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SimCacheStatus.Merge(m, src)
}
func (m *SimCacheStatus) XXX_Size() int {
	return xxx_messageInfo_SimCacheStatus.Size(m)
}
func (m *SimCacheStatus) XXX_DiscardUnknown() {
	xxx_messageInfo_SimCacheStatus.DiscardUnknown(m)
}

var xxx_messageInfo_SimCacheStatus proto.InternalMessageInfo

func (m *SimCacheStatus) GetHitRate() float32 {
	if m != nil {
		return m.HitRate
	}
	return 0
}

func (m *SimCacheStatus) GetWeightedHitRate() float32 {
	if m != nil {
		return m.WeightedHitRate
	}
	return 0
}

func (m *SimCacheStatus) GetHitOverMiss() float32 {
	if m != nil {
		return m.HitOverMiss
	}
	return 0
}

func (m *SimCacheStatus) GetSize() float32 {
	if m != nil {
		return m.Size
	}
	return 0
}

func (m *SimCacheStatus) GetCapacity() float32 {
	if m != nil {
		return m.Capacity
	}
	return 0
}

func (m *SimCacheStatus) GetDataWritten() float32 {
	if m != nil {
		return m.DataWritten
	}
	return 0
}

func (m *SimCacheStatus) GetDataRead() float32 {
	if m != nil {
		return m.DataRead
	}
	return 0
}

func (m *SimCacheStatus) GetDataReadOnHit() float32 {
	if m != nil {
		return m.DataReadOnHit
	}
	return 0
}

func (m *SimCacheStatus) GetDataReadOnMiss() float32 {
	if m != nil {
		return m.DataReadOnMiss
	}
	return 0
}

func (m *SimCacheStatus) GetDataDeleted() float32 {
	if m != nil {
		return m.DataDeleted
	}
	return 0
}

type SimCommonFile struct {
	Filename             string   `protobuf:"bytes,1,opt,name=filename,proto3" json:"filename,omitempty"`
	Size                 float32  `protobuf:"fixed32,2,opt,name=size,proto3" json:"size,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SimCommonFile) Reset()         { *m = SimCommonFile{} }
func (m *SimCommonFile) String() string { return proto.CompactTextString(m) }
func (*SimCommonFile) ProtoMessage()    {}
func (*SimCommonFile) Descriptor() ([]byte, []int) {
	return fileDescriptor_20cb65478f01afe7, []int{2}
}

func (m *SimCommonFile) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SimCommonFile.Unmarshal(m, b)
}
func (m *SimCommonFile) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SimCommonFile.Marshal(b, m, deterministic)
}
func (m *SimCommonFile) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SimCommonFile.Merge(m, src)
}
func (m *SimCommonFile) XXX_Size() int {
	return xxx_messageInfo_SimCommonFile.Size(m)
}
func (m *SimCommonFile) XXX_DiscardUnknown() {
	xxx_messageInfo_SimCommonFile.DiscardUnknown(m)
}

var xxx_messageInfo_SimCommonFile proto.InternalMessageInfo

func (m *SimCommonFile) GetFilename() string {
	if m != nil {
		return m.Filename
	}
	return ""
}

func (m *SimCommonFile) GetSize() float32 {
	if m != nil {
		return m.Size
	}
	return 0
}

type SimDumpRecord struct {
	Raw                  []byte   `protobuf:"bytes,1,opt,name=raw,proto3" json:"raw,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SimDumpRecord) Reset()         { *m = SimDumpRecord{} }
func (m *SimDumpRecord) String() string { return proto.CompactTextString(m) }
func (*SimDumpRecord) ProtoMessage()    {}
func (*SimDumpRecord) Descriptor() ([]byte, []int) {
	return fileDescriptor_20cb65478f01afe7, []int{3}
}

func (m *SimDumpRecord) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SimDumpRecord.Unmarshal(m, b)
}
func (m *SimDumpRecord) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SimDumpRecord.Marshal(b, m, deterministic)
}
func (m *SimDumpRecord) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SimDumpRecord.Merge(m, src)
}
func (m *SimDumpRecord) XXX_Size() int {
	return xxx_messageInfo_SimDumpRecord.Size(m)
}
func (m *SimDumpRecord) XXX_DiscardUnknown() {
	xxx_messageInfo_SimDumpRecord.DiscardUnknown(m)
}

var xxx_messageInfo_SimDumpRecord proto.InternalMessageInfo

func (m *SimDumpRecord) GetRaw() []byte {
	if m != nil {
		return m.Raw
	}
	return nil
}

func init() {
	proto.RegisterType((*ActionResult)(nil), "simservice.ActionResult")
	proto.RegisterType((*SimCacheStatus)(nil), "simservice.SimCacheStatus")
	proto.RegisterType((*SimCommonFile)(nil), "simservice.SimCommonFile")
	proto.RegisterType((*SimDumpRecord)(nil), "simservice.SimDumpRecord")
}

func init() { proto.RegisterFile("simService/simService.proto", fileDescriptor_20cb65478f01afe7) }

var fileDescriptor_20cb65478f01afe7 = []byte{
	// 493 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x92, 0x4d, 0x6e, 0xdb, 0x30,
	0x10, 0x85, 0x2d, 0x3b, 0x71, 0x9c, 0x69, 0x7e, 0x0a, 0x22, 0x2d, 0x18, 0x65, 0xe3, 0x0a, 0x45,
	0xe1, 0x15, 0x5d, 0xb4, 0x07, 0xc8, 0x4f, 0x93, 0xd6, 0x05, 0x12, 0xa4, 0x90, 0x16, 0x5d, 0x33,
	0xd2, 0xd8, 0x26, 0x20, 0x8a, 0x82, 0x48, 0x27, 0x48, 0xcf, 0xd9, 0xcb, 0x74, 0x57, 0x90, 0xb4,
	0x2c, 0xc5, 0x80, 0xb3, 0xf0, 0x8e, 0xf3, 0xf8, 0xf8, 0x69, 0x34, 0xf3, 0xe0, 0x4c, 0x0b, 0x99,
	0x60, 0xf5, 0x28, 0x52, 0x1c, 0x37, 0x47, 0x56, 0x56, 0xca, 0x28, 0x02, 0x5a, 0x48, 0xed, 0x95,
	0xf0, 0x6c, 0xa6, 0xd4, 0x2c, 0xc7, 0xb1, 0xbb, 0x79, 0x58, 0x4c, 0xc7, 0x28, 0x4b, 0xf3, 0xec,
	0x8d, 0xd1, 0x05, 0x1c, 0x5c, 0xa6, 0x46, 0xa8, 0x22, 0x46, 0xbd, 0xc8, 0x0d, 0x09, 0x61, 0x30,
	0x15, 0x39, 0x16, 0x5c, 0x22, 0x0d, 0x86, 0xc1, 0x68, 0x3f, 0x5e, 0xd5, 0xe4, 0x04, 0x76, 0x79,
	0x96, 0x61, 0x46, 0xbb, 0xc3, 0x60, 0x34, 0x88, 0x7d, 0x11, 0xfd, 0xed, 0xc2, 0x51, 0x22, 0xe4,
	0x37, 0x9e, 0xce, 0x31, 0x31, 0xdc, 0x2c, 0x34, 0xa1, 0xb0, 0x37, 0x17, 0x26, 0xe6, 0xc6, 0x33,
	0xba, 0x71, 0x5d, 0x92, 0x11, 0x1c, 0x3f, 0xa1, 0x98, 0xcd, 0x0d, 0x66, 0x93, 0xa5, 0xa3, 0xeb,
	0x1c, 0xeb, 0x32, 0x19, 0xc2, 0x9b, 0xb9, 0x30, 0xf7, 0x8f, 0x58, 0xdd, 0x09, 0xad, 0x69, 0xcf,
	0xb9, 0xda, 0x12, 0x21, 0xb0, 0xa3, 0xc5, 0x1f, 0xa4, 0x3b, 0xee, 0xca, 0x9d, 0x6d, 0xfb, 0x29,
	0x2f, 0x79, 0x2a, 0xcc, 0x33, 0xdd, 0x75, 0xfa, 0xaa, 0xb6, 0xc4, 0x8c, 0x1b, 0xfe, 0xbb, 0x12,
	0xc6, 0x60, 0x41, 0xfb, 0x9e, 0xd8, 0x92, 0xec, 0x6b, 0x5b, 0xc6, 0xc8, 0x33, 0xba, 0xe7, 0x5f,
	0xd7, 0x35, 0xf9, 0x08, 0x87, 0xf5, 0xf9, 0xbe, 0x98, 0x08, 0x43, 0x07, 0xce, 0xf0, 0x52, 0x24,
	0x9f, 0xe0, 0xa8, 0x11, 0x5c, 0xe3, 0xfb, 0xce, 0xb6, 0xa6, 0xd6, 0xbd, 0x5c, 0x63, 0x8e, 0x06,
	0x33, 0x0a, 0x4d, 0x2f, 0x4b, 0x29, 0x3a, 0x87, 0x43, 0x3b, 0x55, 0x25, 0xa5, 0x2a, 0xbe, 0x8b,
	0x1c, 0x5f, 0xdd, 0x4c, 0x3d, 0x8a, 0x6e, 0x33, 0x8a, 0xe8, 0x83, 0x03, 0x5c, 0x2f, 0x64, 0x19,
	0x63, 0xaa, 0xaa, 0x8c, 0xbc, 0x85, 0x5e, 0xc5, 0x9f, 0xdc, 0xdb, 0x83, 0xd8, 0x1e, 0xbf, 0xfc,
	0xeb, 0x01, 0x24, 0xab, 0xe8, 0x90, 0x73, 0xe8, 0x27, 0x42, 0xfe, 0x40, 0x43, 0x4e, 0x59, 0x93,
	0x1f, 0xf6, 0xa2, 0x8d, 0x90, 0xb6, 0xaf, 0xda, 0xd1, 0x89, 0x3a, 0xe4, 0x02, 0x06, 0xd6, 0x9c,
	0x23, 0xaf, 0xc8, 0x7b, 0xe6, 0x63, 0xc7, 0xea, 0xd8, 0xb1, 0x1b, 0x1b, 0xbb, 0x30, 0x5c, 0x47,
	0x37, 0xb9, 0x89, 0x3a, 0xe4, 0xc6, 0xff, 0xb5, 0x25, 0xd8, 0xaf, 0xe9, 0x2d, 0x31, 0xb7, 0x70,
	0x52, 0x63, 0x26, 0xc2, 0xd8, 0x89, 0xdb, 0xab, 0x6d, 0x69, 0x77, 0xf0, 0xce, 0xcf, 0xe5, 0x67,
	0x31, 0x55, 0xed, 0x9c, 0x6f, 0x87, 0xbb, 0x74, 0x53, 0xb2, 0x8b, 0xd9, 0x4c, 0x58, 0x5f, 0x40,
	0xb3, 0xc6, 0xa8, 0xf3, 0x39, 0x58, 0x22, 0x6e, 0x15, 0xcf, 0x34, 0xd9, 0x6c, 0x0d, 0x37, 0xd0,
	0xa3, 0xce, 0x28, 0xb8, 0x62, 0x70, 0x2a, 0x14, 0x9b, 0x55, 0x65, 0xca, 0x52, 0xdb, 0x9e, 0xa5,
	0xb0, 0x25, 0xe6, 0xea, 0xd8, 0x77, 0xbc, 0x8a, 0xc6, 0xaf, 0xe0, 0xa1, 0xef, 0x18, 0x5f, 0xff,
	0x07, 0x00, 0x00, 0xff, 0xff, 0xdb, 0xe7, 0x64, 0x03, 0x77, 0x04, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// SimServiceClient is the client API for SimService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type SimServiceClient interface {
	// Requeste a file to the simulated cache
	SimGet(ctx context.Context, in *SimCommonFile, opts ...grpc.CallOption) (*ActionResult, error)
	// Clear the cache, files, statistics and so on.
	SimClear(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error)
	// Clear only the files in the cache
	SimClearFiles(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error)
	// Reset only the statistics of the simulated cache
	SimClearHitMissStats(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error)
	// Retrieve the simulated cache status
	SimGetInfoCacheStatus(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error)
	// Save the state of the current simulated cache
	SimDumps(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (SimService_SimDumpsClient, error)
	// Load a previuos saved state of the cache
	SimLoads(ctx context.Context, opts ...grpc.CallOption) (SimService_SimLoadsClient, error)
}

type simServiceClient struct {
	cc *grpc.ClientConn
}

func NewSimServiceClient(cc *grpc.ClientConn) SimServiceClient {
	return &simServiceClient{cc}
}

func (c *simServiceClient) SimGet(ctx context.Context, in *SimCommonFile, opts ...grpc.CallOption) (*ActionResult, error) {
	out := new(ActionResult)
	err := c.cc.Invoke(ctx, "/simservice.SimService/SimGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simServiceClient) SimClear(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error) {
	out := new(SimCacheStatus)
	err := c.cc.Invoke(ctx, "/simservice.SimService/SimClear", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simServiceClient) SimClearFiles(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error) {
	out := new(SimCacheStatus)
	err := c.cc.Invoke(ctx, "/simservice.SimService/SimClearFiles", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simServiceClient) SimClearHitMissStats(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error) {
	out := new(SimCacheStatus)
	err := c.cc.Invoke(ctx, "/simservice.SimService/SimClearHitMissStats", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simServiceClient) SimGetInfoCacheStatus(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error) {
	out := new(SimCacheStatus)
	err := c.cc.Invoke(ctx, "/simservice.SimService/SimGetInfoCacheStatus", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simServiceClient) SimDumps(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (SimService_SimDumpsClient, error) {
	stream, err := c.cc.NewStream(ctx, &_SimService_serviceDesc.Streams[0], "/simservice.SimService/SimDumps", opts...)
	if err != nil {
		return nil, err
	}
	x := &simServiceSimDumpsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type SimService_SimDumpsClient interface {
	Recv() (*SimDumpRecord, error)
	grpc.ClientStream
}

type simServiceSimDumpsClient struct {
	grpc.ClientStream
}

func (x *simServiceSimDumpsClient) Recv() (*SimDumpRecord, error) {
	m := new(SimDumpRecord)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *simServiceClient) SimLoads(ctx context.Context, opts ...grpc.CallOption) (SimService_SimLoadsClient, error) {
	stream, err := c.cc.NewStream(ctx, &_SimService_serviceDesc.Streams[1], "/simservice.SimService/SimLoads", opts...)
	if err != nil {
		return nil, err
	}
	x := &simServiceSimLoadsClient{stream}
	return x, nil
}

type SimService_SimLoadsClient interface {
	Send(*SimDumpRecord) error
	CloseAndRecv() (*empty.Empty, error)
	grpc.ClientStream
}

type simServiceSimLoadsClient struct {
	grpc.ClientStream
}

func (x *simServiceSimLoadsClient) Send(m *SimDumpRecord) error {
	return x.ClientStream.SendMsg(m)
}

func (x *simServiceSimLoadsClient) CloseAndRecv() (*empty.Empty, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(empty.Empty)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// SimServiceServer is the server API for SimService service.
type SimServiceServer interface {
	// Requeste a file to the simulated cache
	SimGet(context.Context, *SimCommonFile) (*ActionResult, error)
	// Clear the cache, files, statistics and so on.
	SimClear(context.Context, *empty.Empty) (*SimCacheStatus, error)
	// Clear only the files in the cache
	SimClearFiles(context.Context, *empty.Empty) (*SimCacheStatus, error)
	// Reset only the statistics of the simulated cache
	SimClearHitMissStats(context.Context, *empty.Empty) (*SimCacheStatus, error)
	// Retrieve the simulated cache status
	SimGetInfoCacheStatus(context.Context, *empty.Empty) (*SimCacheStatus, error)
	// Save the state of the current simulated cache
	SimDumps(*empty.Empty, SimService_SimDumpsServer) error
	// Load a previuos saved state of the cache
	SimLoads(SimService_SimLoadsServer) error
}

// UnimplementedSimServiceServer can be embedded to have forward compatible implementations.
type UnimplementedSimServiceServer struct {
}

func (*UnimplementedSimServiceServer) SimGet(ctx context.Context, req *SimCommonFile) (*ActionResult, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SimGet not implemented")
}
func (*UnimplementedSimServiceServer) SimClear(ctx context.Context, req *empty.Empty) (*SimCacheStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SimClear not implemented")
}
func (*UnimplementedSimServiceServer) SimClearFiles(ctx context.Context, req *empty.Empty) (*SimCacheStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SimClearFiles not implemented")
}
func (*UnimplementedSimServiceServer) SimClearHitMissStats(ctx context.Context, req *empty.Empty) (*SimCacheStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SimClearHitMissStats not implemented")
}
func (*UnimplementedSimServiceServer) SimGetInfoCacheStatus(ctx context.Context, req *empty.Empty) (*SimCacheStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SimGetInfoCacheStatus not implemented")
}
func (*UnimplementedSimServiceServer) SimDumps(req *empty.Empty, srv SimService_SimDumpsServer) error {
	return status.Errorf(codes.Unimplemented, "method SimDumps not implemented")
}
func (*UnimplementedSimServiceServer) SimLoads(srv SimService_SimLoadsServer) error {
	return status.Errorf(codes.Unimplemented, "method SimLoads not implemented")
}

func RegisterSimServiceServer(s *grpc.Server, srv SimServiceServer) {
	s.RegisterService(&_SimService_serviceDesc, srv)
}

func _SimService_SimGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SimCommonFile)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimServiceServer).SimGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/simservice.SimService/SimGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimServiceServer).SimGet(ctx, req.(*SimCommonFile))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimService_SimClear_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimServiceServer).SimClear(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/simservice.SimService/SimClear",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimServiceServer).SimClear(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimService_SimClearFiles_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimServiceServer).SimClearFiles(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/simservice.SimService/SimClearFiles",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimServiceServer).SimClearFiles(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimService_SimClearHitMissStats_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimServiceServer).SimClearHitMissStats(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/simservice.SimService/SimClearHitMissStats",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimServiceServer).SimClearHitMissStats(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimService_SimGetInfoCacheStatus_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimServiceServer).SimGetInfoCacheStatus(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/simservice.SimService/SimGetInfoCacheStatus",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimServiceServer).SimGetInfoCacheStatus(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimService_SimDumps_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(empty.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(SimServiceServer).SimDumps(m, &simServiceSimDumpsServer{stream})
}

type SimService_SimDumpsServer interface {
	Send(*SimDumpRecord) error
	grpc.ServerStream
}

type simServiceSimDumpsServer struct {
	grpc.ServerStream
}

func (x *simServiceSimDumpsServer) Send(m *SimDumpRecord) error {
	return x.ServerStream.SendMsg(m)
}

func _SimService_SimLoads_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(SimServiceServer).SimLoads(&simServiceSimLoadsServer{stream})
}

type SimService_SimLoadsServer interface {
	SendAndClose(*empty.Empty) error
	Recv() (*SimDumpRecord, error)
	grpc.ServerStream
}

type simServiceSimLoadsServer struct {
	grpc.ServerStream
}

func (x *simServiceSimLoadsServer) SendAndClose(m *empty.Empty) error {
	return x.ServerStream.SendMsg(m)
}

func (x *simServiceSimLoadsServer) Recv() (*SimDumpRecord, error) {
	m := new(SimDumpRecord)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _SimService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "simservice.SimService",
	HandlerType: (*SimServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SimGet",
			Handler:    _SimService_SimGet_Handler,
		},
		{
			MethodName: "SimClear",
			Handler:    _SimService_SimClear_Handler,
		},
		{
			MethodName: "SimClearFiles",
			Handler:    _SimService_SimClearFiles_Handler,
		},
		{
			MethodName: "SimClearHitMissStats",
			Handler:    _SimService_SimClearHitMissStats_Handler,
		},
		{
			MethodName: "SimGetInfoCacheStatus",
			Handler:    _SimService_SimGetInfoCacheStatus_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "SimDumps",
			Handler:       _SimService_SimDumps_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SimLoads",
			Handler:       _SimService_SimLoads_Handler,
			ClientStreams: true,
		},
	},
	Metadata: "simService/simService.proto",
}
