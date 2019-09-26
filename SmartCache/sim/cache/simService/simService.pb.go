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
	WrittenData          float32  `protobuf:"fixed32,6,opt,name=writtenData,proto3" json:"writtenData,omitempty"`
	ReadOnHit            float32  `protobuf:"fixed32,7,opt,name=readOnHit,proto3" json:"readOnHit,omitempty"`
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

func (m *SimCacheStatus) GetWrittenData() float32 {
	if m != nil {
		return m.WrittenData
	}
	return 0
}

func (m *SimCacheStatus) GetReadOnHit() float32 {
	if m != nil {
		return m.ReadOnHit
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
	// 459 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x92, 0xdf, 0x6e, 0xd3, 0x30,
	0x14, 0xc6, 0x9b, 0x76, 0xeb, 0xba, 0xc3, 0x60, 0xc8, 0x1a, 0x28, 0xcb, 0xb8, 0x28, 0xb9, 0xea,
	0x95, 0x8b, 0xe0, 0x01, 0xf6, 0x87, 0x0d, 0x8a, 0xb4, 0x69, 0x28, 0x79, 0x02, 0x2f, 0x39, 0x6d,
	0x8f, 0x94, 0xc4, 0x91, 0xed, 0xae, 0x1a, 0x0f, 0xcc, 0x3b, 0x70, 0x87, 0x6c, 0xb7, 0x4d, 0xa8,
	0x54, 0x2e, 0x7a, 0xe7, 0x73, 0xce, 0x97, 0x5f, 0x3e, 0xfb, 0x3b, 0x70, 0xa1, 0xa9, 0x4c, 0x51,
	0x3d, 0x53, 0x86, 0xe3, 0xe6, 0xc8, 0x6b, 0x25, 0x8d, 0x64, 0xa0, 0xa9, 0xd4, 0xbe, 0x13, 0x5d,
	0xcc, 0xa4, 0x9c, 0x15, 0x38, 0x76, 0x93, 0xa7, 0xc5, 0x74, 0x8c, 0x65, 0x6d, 0x5e, 0xbc, 0x30,
	0xbe, 0x82, 0x93, 0xeb, 0xcc, 0x90, 0xac, 0x12, 0xd4, 0x8b, 0xc2, 0xb0, 0x08, 0x06, 0x53, 0x2a,
	0xb0, 0x12, 0x25, 0x86, 0xc1, 0x30, 0x18, 0x1d, 0x27, 0x9b, 0x9a, 0x9d, 0xc1, 0xa1, 0xc8, 0x73,
	0xcc, 0xc3, 0xee, 0x30, 0x18, 0x0d, 0x12, 0x5f, 0xc4, 0xbf, 0x03, 0x78, 0x93, 0x52, 0xf9, 0x55,
	0x64, 0x73, 0x4c, 0x8d, 0x30, 0x0b, 0xcd, 0x42, 0x38, 0x9a, 0x93, 0x49, 0x84, 0xf1, 0x8c, 0x6e,
	0xb2, 0x2e, 0xd9, 0x08, 0x4e, 0x97, 0x48, 0xb3, 0xb9, 0xc1, 0x7c, 0xb2, 0x52, 0x74, 0x9d, 0x62,
	0xbb, 0xcd, 0x86, 0xf0, 0x6a, 0x4e, 0xe6, 0xf1, 0x19, 0xd5, 0x03, 0x69, 0x1d, 0xf6, 0x9c, 0xaa,
	0xdd, 0x62, 0x0c, 0x0e, 0x34, 0xfd, 0xc2, 0xf0, 0xc0, 0x8d, 0xdc, 0xd9, 0xda, 0xcf, 0x44, 0x2d,
	0x32, 0x32, 0x2f, 0xe1, 0xa1, 0xeb, 0x6f, 0x6a, 0x4b, 0x5c, 0x2a, 0x32, 0x06, 0xab, 0x5b, 0x61,
	0x44, 0xd8, 0xf7, 0xc4, 0x56, 0x8b, 0x7d, 0x80, 0x63, 0x85, 0x22, 0x7f, 0xac, 0x26, 0x64, 0xc2,
	0x23, 0x37, 0x6f, 0x1a, 0xf1, 0x25, 0xbc, 0xb6, 0xf7, 0x94, 0x65, 0x29, 0xab, 0x6f, 0x54, 0xe0,
	0x7f, 0xdf, 0x6a, 0x6d, 0xae, 0xdb, 0x98, 0x8b, 0x3f, 0x3a, 0xc0, 0xed, 0xa2, 0xac, 0x13, 0xcc,
	0xa4, 0xca, 0xd9, 0x5b, 0xe8, 0x29, 0xb1, 0x74, 0xdf, 0x9e, 0x24, 0xf6, 0xf8, 0xf9, 0x4f, 0x0f,
	0x20, 0xdd, 0x84, 0xc9, 0x2e, 0xa1, 0x9f, 0x52, 0xf9, 0x1d, 0x0d, 0x3b, 0xe7, 0x4d, 0xa2, 0xfc,
	0x1f, 0x1b, 0x51, 0xd8, 0x1e, 0xb5, 0xc3, 0x8c, 0x3b, 0xec, 0x0a, 0x06, 0x56, 0x5c, 0xa0, 0x50,
	0xec, 0x3d, 0xf7, 0x8b, 0xc0, 0xd7, 0x8b, 0xc0, 0xef, 0xec, 0x22, 0x44, 0xd1, 0x36, 0xba, 0x49,
	0x32, 0xee, 0xb0, 0x3b, 0x7f, 0x6b, 0x4b, 0xb0, 0x7f, 0xd3, 0x7b, 0x62, 0xee, 0xe1, 0x6c, 0x8d,
	0x99, 0x90, 0xb1, 0xf9, 0xd9, 0xd1, 0xbe, 0xb4, 0x07, 0x78, 0xe7, 0xdf, 0xe5, 0x47, 0x35, 0x95,
	0xed, 0xcd, 0xdb, 0x0f, 0x77, 0xed, 0x5e, 0xc9, 0x06, 0xb3, 0x9b, 0xb0, 0x1d, 0x40, 0x13, 0x63,
	0xdc, 0xf9, 0x14, 0xac, 0x10, 0xf7, 0x52, 0xe4, 0x9a, 0xed, 0x96, 0x46, 0x3b, 0xe8, 0x71, 0x67,
	0x14, 0xdc, 0x70, 0x38, 0x27, 0xc9, 0x67, 0xaa, 0xce, 0x78, 0x66, 0xed, 0x59, 0x0a, 0x5f, 0x61,
	0x6e, 0x4e, 0xbd, 0xe3, 0xcd, 0x6a, 0xfc, 0x0c, 0x9e, 0xfa, 0x8e, 0xf1, 0xe5, 0x6f, 0x00, 0x00,
	0x00, 0xff, 0xff, 0x8e, 0x74, 0xc6, 0xe5, 0x09, 0x04, 0x00, 0x00,
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
	SimGet(ctx context.Context, in *SimCommonFile, opts ...grpc.CallOption) (*ActionResult, error)
	SimClear(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error)
	SimClearFiles(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error)
	SimClearHitMissStats(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error)
	SimGetInfoCacheStatus(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error)
	SimDumps(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (SimService_SimDumpsClient, error)
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
	SimGet(context.Context, *SimCommonFile) (*ActionResult, error)
	SimClear(context.Context, *empty.Empty) (*SimCacheStatus, error)
	SimClearFiles(context.Context, *empty.Empty) (*SimCacheStatus, error)
	SimClearHitMissStats(context.Context, *empty.Empty) (*SimCacheStatus, error)
	SimGetInfoCacheStatus(context.Context, *empty.Empty) (*SimCacheStatus, error)
	SimDumps(*empty.Empty, SimService_SimDumpsServer) error
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
