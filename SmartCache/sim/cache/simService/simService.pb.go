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
	return fileDescriptor_20cb65478f01afe7, []int{0}
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

type SimCacheStatus struct {
	HitRate              float32  `protobuf:"fixed32,1,opt,name=hitRate,proto3" json:"hitRate,omitempty"`
	Size                 float32  `protobuf:"fixed32,2,opt,name=size,proto3" json:"size,omitempty"`
	WrittenData          float32  `protobuf:"fixed32,3,opt,name=writtenData,proto3" json:"writtenData,omitempty"`
	Capacity             float32  `protobuf:"fixed32,4,opt,name=capacity,proto3" json:"capacity,omitempty"`
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

func (m *SimCacheStatus) GetSize() float32 {
	if m != nil {
		return m.Size
	}
	return 0
}

func (m *SimCacheStatus) GetWrittenData() float32 {
	if m != nil {
		return m.WrittenData
	}
	return 0
}

func (m *SimCacheStatus) GetCapacity() float32 {
	if m != nil {
		return m.Capacity
	}
	return 0
}

type SimFileWeight struct {
	Filename             string   `protobuf:"bytes,1,opt,name=filename,proto3" json:"filename,omitempty"`
	Weight               float32  `protobuf:"fixed32,2,opt,name=weight,proto3" json:"weight,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SimFileWeight) Reset()         { *m = SimFileWeight{} }
func (m *SimFileWeight) String() string { return proto.CompactTextString(m) }
func (*SimFileWeight) ProtoMessage()    {}
func (*SimFileWeight) Descriptor() ([]byte, []int) {
	return fileDescriptor_20cb65478f01afe7, []int{2}
}

func (m *SimFileWeight) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_SimFileWeight.Unmarshal(m, b)
}
func (m *SimFileWeight) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_SimFileWeight.Marshal(b, m, deterministic)
}
func (m *SimFileWeight) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SimFileWeight.Merge(m, src)
}
func (m *SimFileWeight) XXX_Size() int {
	return xxx_messageInfo_SimFileWeight.Size(m)
}
func (m *SimFileWeight) XXX_DiscardUnknown() {
	xxx_messageInfo_SimFileWeight.DiscardUnknown(m)
}

var xxx_messageInfo_SimFileWeight proto.InternalMessageInfo

func (m *SimFileWeight) GetFilename() string {
	if m != nil {
		return m.Filename
	}
	return ""
}

func (m *SimFileWeight) GetWeight() float32 {
	if m != nil {
		return m.Weight
	}
	return 0
}

func init() {
	proto.RegisterType((*SimCommonFile)(nil), "simservice.SimCommonFile")
	proto.RegisterType((*SimCacheStatus)(nil), "simservice.SimCacheStatus")
	proto.RegisterType((*SimFileWeight)(nil), "simservice.SimFileWeight")
}

func init() { proto.RegisterFile("simService/simService.proto", fileDescriptor_20cb65478f01afe7) }

var fileDescriptor_20cb65478f01afe7 = []byte{
	// 342 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x8c, 0x91, 0x51, 0x4f, 0xfa, 0x30,
	0x14, 0xc5, 0xd9, 0xfe, 0x84, 0xbf, 0x5e, 0xa3, 0x24, 0x7d, 0x20, 0x63, 0xc4, 0x84, 0xec, 0x89,
	0xa7, 0x62, 0xf4, 0x03, 0x98, 0x80, 0x8a, 0xbe, 0x99, 0xcd, 0xe8, 0x73, 0x59, 0x2e, 0xa3, 0xc9,
	0xba, 0x2e, 0x5b, 0x91, 0x60, 0xfc, 0x8a, 0x7e, 0x27, 0xd3, 0x76, 0x6c, 0xa2, 0xa2, 0xbe, 0xf5,
	0xf4, 0x9e, 0xfd, 0x76, 0x7a, 0x2e, 0x0c, 0x4a, 0x2e, 0x22, 0x2c, 0x9e, 0x79, 0x8c, 0xe3, 0xe6,
	0x48, 0xf3, 0x42, 0x2a, 0x49, 0xa0, 0xe4, 0xa2, 0xb4, 0x37, 0xfe, 0x20, 0x91, 0x32, 0x49, 0x71,
	0x6c, 0x26, 0xf3, 0xd5, 0x62, 0x8c, 0x22, 0x57, 0x1b, 0x6b, 0x0c, 0x2e, 0xe1, 0x38, 0xe2, 0x62,
	0x2a, 0x85, 0x90, 0xd9, 0x0d, 0x4f, 0x91, 0xf8, 0x70, 0xb0, 0xe0, 0x29, 0x66, 0x4c, 0xa0, 0xe7,
	0x0c, 0x9d, 0xd1, 0x61, 0x58, 0x6b, 0x42, 0xa0, 0x5d, 0xf2, 0x17, 0xf4, 0xdc, 0xa1, 0x33, 0x72,
	0x43, 0x73, 0x0e, 0x5e, 0xe1, 0x44, 0x03, 0x58, 0xbc, 0xc4, 0x48, 0x31, 0xb5, 0x2a, 0x89, 0x07,
	0xff, 0x97, 0x5c, 0x85, 0x4c, 0x59, 0x80, 0x1b, 0x6e, 0xe5, 0x77, 0xdf, 0x93, 0x21, 0x1c, 0xad,
	0x0b, 0xae, 0x14, 0x66, 0x57, 0x4c, 0x31, 0xef, 0x9f, 0x19, 0x7d, 0xbc, 0xd2, 0x89, 0x62, 0x96,
	0xb3, 0x98, 0xab, 0x8d, 0xd7, 0x36, 0xe3, 0x5a, 0x07, 0x53, 0x13, 0x5f, 0x07, 0x7f, 0x42, 0x9e,
	0x2c, 0xd5, 0x8f, 0xf1, 0x7b, 0xd0, 0x59, 0x1b, 0x57, 0x15, 0xa0, 0x52, 0xe7, 0x6f, 0x2e, 0x40,
	0x54, 0x37, 0x48, 0x6e, 0x0d, 0xb3, 0x52, 0x33, 0x54, 0xa4, 0x4f, 0x9b, 0x36, 0xe9, 0x4e, 0x5b,
	0xbe, 0xff, 0x79, 0xd4, 0xf4, 0x10, 0xb4, 0xc8, 0x0c, 0xba, 0x0d, 0x69, 0x9a, 0x22, 0x2b, 0x48,
	0x8f, 0xda, 0x6d, 0xd0, 0xed, 0x36, 0xe8, 0xb5, 0xde, 0xc6, 0x2f, 0xa0, 0x07, 0x18, 0xec, 0x44,
	0xba, 0xcb, 0x16, 0xd2, 0x38, 0x74, 0x88, 0x72, 0x2f, 0x74, 0x7f, 0xf0, 0xa0, 0x75, 0xe6, 0x90,
	0x47, 0x38, 0xfd, 0x42, 0x35, 0x40, 0xdb, 0xe5, 0xdf, 0xb9, 0x4d, 0xff, 0x9a, 0x3b, 0xa1, 0xd0,
	0xe7, 0x92, 0x26, 0x45, 0x1e, 0xd3, 0x58, 0x87, 0xd4, 0x6e, 0x5a, 0xd9, 0x27, 0x5d, 0xfb, 0xb2,
	0xfa, 0xbf, 0xf7, 0xce, 0xbc, 0x63, 0xf0, 0x17, 0xef, 0x01, 0x00, 0x00, 0xff, 0xff, 0xef, 0xda,
	0x4e, 0x23, 0xd2, 0x02, 0x00, 0x00,
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
	SimServiceGet(ctx context.Context, in *SimCommonFile, opts ...grpc.CallOption) (*SimCacheStatus, error)
	SimServiceClear(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error)
	SimServiceGetInfoCacheFiles(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (SimService_SimServiceGetInfoCacheFilesClient, error)
	SimServiceGetInfoFilesWeights(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (SimService_SimServiceGetInfoFilesWeightsClient, error)
}

type simServiceClient struct {
	cc *grpc.ClientConn
}

func NewSimServiceClient(cc *grpc.ClientConn) SimServiceClient {
	return &simServiceClient{cc}
}

func (c *simServiceClient) SimServiceGet(ctx context.Context, in *SimCommonFile, opts ...grpc.CallOption) (*SimCacheStatus, error) {
	out := new(SimCacheStatus)
	err := c.cc.Invoke(ctx, "/simservice.SimService/SimServiceGet", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simServiceClient) SimServiceClear(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error) {
	out := new(SimCacheStatus)
	err := c.cc.Invoke(ctx, "/simservice.SimService/SimServiceClear", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simServiceClient) SimServiceGetInfoCacheFiles(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (SimService_SimServiceGetInfoCacheFilesClient, error) {
	stream, err := c.cc.NewStream(ctx, &_SimService_serviceDesc.Streams[0], "/simservice.SimService/SimServiceGetInfoCacheFiles", opts...)
	if err != nil {
		return nil, err
	}
	x := &simServiceSimServiceGetInfoCacheFilesClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type SimService_SimServiceGetInfoCacheFilesClient interface {
	Recv() (*SimCommonFile, error)
	grpc.ClientStream
}

type simServiceSimServiceGetInfoCacheFilesClient struct {
	grpc.ClientStream
}

func (x *simServiceSimServiceGetInfoCacheFilesClient) Recv() (*SimCommonFile, error) {
	m := new(SimCommonFile)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *simServiceClient) SimServiceGetInfoFilesWeights(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (SimService_SimServiceGetInfoFilesWeightsClient, error) {
	stream, err := c.cc.NewStream(ctx, &_SimService_serviceDesc.Streams[1], "/simservice.SimService/SimServiceGetInfoFilesWeights", opts...)
	if err != nil {
		return nil, err
	}
	x := &simServiceSimServiceGetInfoFilesWeightsClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type SimService_SimServiceGetInfoFilesWeightsClient interface {
	Recv() (*SimFileWeight, error)
	grpc.ClientStream
}

type simServiceSimServiceGetInfoFilesWeightsClient struct {
	grpc.ClientStream
}

func (x *simServiceSimServiceGetInfoFilesWeightsClient) Recv() (*SimFileWeight, error) {
	m := new(SimFileWeight)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// SimServiceServer is the server API for SimService service.
type SimServiceServer interface {
	SimServiceGet(context.Context, *SimCommonFile) (*SimCacheStatus, error)
	SimServiceClear(context.Context, *empty.Empty) (*SimCacheStatus, error)
	SimServiceGetInfoCacheFiles(*empty.Empty, SimService_SimServiceGetInfoCacheFilesServer) error
	SimServiceGetInfoFilesWeights(*empty.Empty, SimService_SimServiceGetInfoFilesWeightsServer) error
}

// UnimplementedSimServiceServer can be embedded to have forward compatible implementations.
type UnimplementedSimServiceServer struct {
}

func (*UnimplementedSimServiceServer) SimServiceGet(ctx context.Context, req *SimCommonFile) (*SimCacheStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SimServiceGet not implemented")
}
func (*UnimplementedSimServiceServer) SimServiceClear(ctx context.Context, req *empty.Empty) (*SimCacheStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SimServiceClear not implemented")
}
func (*UnimplementedSimServiceServer) SimServiceGetInfoCacheFiles(req *empty.Empty, srv SimService_SimServiceGetInfoCacheFilesServer) error {
	return status.Errorf(codes.Unimplemented, "method SimServiceGetInfoCacheFiles not implemented")
}
func (*UnimplementedSimServiceServer) SimServiceGetInfoFilesWeights(req *empty.Empty, srv SimService_SimServiceGetInfoFilesWeightsServer) error {
	return status.Errorf(codes.Unimplemented, "method SimServiceGetInfoFilesWeights not implemented")
}

func RegisterSimServiceServer(s *grpc.Server, srv SimServiceServer) {
	s.RegisterService(&_SimService_serviceDesc, srv)
}

func _SimService_SimServiceGet_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(SimCommonFile)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimServiceServer).SimServiceGet(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/simservice.SimService/SimServiceGet",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimServiceServer).SimServiceGet(ctx, req.(*SimCommonFile))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimService_SimServiceClear_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(empty.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimServiceServer).SimServiceClear(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/simservice.SimService/SimServiceClear",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimServiceServer).SimServiceClear(ctx, req.(*empty.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimService_SimServiceGetInfoCacheFiles_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(empty.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(SimServiceServer).SimServiceGetInfoCacheFiles(m, &simServiceSimServiceGetInfoCacheFilesServer{stream})
}

type SimService_SimServiceGetInfoCacheFilesServer interface {
	Send(*SimCommonFile) error
	grpc.ServerStream
}

type simServiceSimServiceGetInfoCacheFilesServer struct {
	grpc.ServerStream
}

func (x *simServiceSimServiceGetInfoCacheFilesServer) Send(m *SimCommonFile) error {
	return x.ServerStream.SendMsg(m)
}

func _SimService_SimServiceGetInfoFilesWeights_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(empty.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(SimServiceServer).SimServiceGetInfoFilesWeights(m, &simServiceSimServiceGetInfoFilesWeightsServer{stream})
}

type SimService_SimServiceGetInfoFilesWeightsServer interface {
	Send(*SimFileWeight) error
	grpc.ServerStream
}

type simServiceSimServiceGetInfoFilesWeightsServer struct {
	grpc.ServerStream
}

func (x *simServiceSimServiceGetInfoFilesWeightsServer) Send(m *SimFileWeight) error {
	return x.ServerStream.SendMsg(m)
}

var _SimService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "simservice.SimService",
	HandlerType: (*SimServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SimServiceGet",
			Handler:    _SimService_SimServiceGet_Handler,
		},
		{
			MethodName: "SimServiceClear",
			Handler:    _SimService_SimServiceClear_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "SimServiceGetInfoCacheFiles",
			Handler:       _SimService_SimServiceGetInfoCacheFiles_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "SimServiceGetInfoFilesWeights",
			Handler:       _SimService_SimServiceGetInfoFilesWeights_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "simService/simService.proto",
}
