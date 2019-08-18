// Code generated by protoc-gen-go. DO NOT EDIT.
// source: simService/simService.proto

package simservice

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
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

func init() {
	proto.RegisterType((*SimCommonFile)(nil), "simservice.SimCommonFile")
	proto.RegisterType((*SimCacheStatus)(nil), "simservice.SimCacheStatus")
}

func init() { proto.RegisterFile("simService/simService.proto", fileDescriptor_20cb65478f01afe7) }

var fileDescriptor_20cb65478f01afe7 = []byte{
	// 226 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x6c, 0x50, 0x4d, 0x4b, 0xc4, 0x30,
	0x10, 0xb5, 0x55, 0xfc, 0x18, 0x51, 0x61, 0x4e, 0xdd, 0x7a, 0x29, 0x3d, 0xed, 0x29, 0x82, 0xfe,
	0x00, 0x61, 0x15, 0xf5, 0x28, 0x09, 0x78, 0x36, 0x86, 0xd1, 0x1d, 0xd8, 0x34, 0x4b, 0x32, 0x2a,
	0xf8, 0xeb, 0xa5, 0xe9, 0xda, 0xaa, 0xec, 0x6d, 0xde, 0xbc, 0x97, 0x97, 0xf7, 0x06, 0xce, 0x13,
	0x7b, 0x43, 0xf1, 0x83, 0x1d, 0x5d, 0x4c, 0xa3, 0x5a, 0xc7, 0x20, 0x01, 0x21, 0xb1, 0x4f, 0xc3,
	0xa6, 0xbd, 0x86, 0x13, 0xc3, 0xfe, 0x26, 0x78, 0x1f, 0xba, 0x3b, 0x5e, 0x11, 0xd6, 0x70, 0xf8,
	0xca, 0x2b, 0xea, 0xac, 0xa7, 0xaa, 0x68, 0x8a, 0xf9, 0x91, 0x1e, 0x31, 0x22, 0xec, 0x25, 0xfe,
	0xa2, 0xaa, 0x6c, 0x8a, 0x79, 0xa9, 0xf3, 0xdc, 0x3e, 0xc3, 0x69, 0x6f, 0x60, 0xdd, 0x92, 0x8c,
	0x58, 0x79, 0x4f, 0x58, 0xc1, 0xc1, 0x92, 0x45, 0x5b, 0x19, 0x0c, 0x4a, 0xfd, 0x03, 0xb7, 0xbd,
	0xc7, 0x06, 0x8e, 0x3f, 0x23, 0x8b, 0x50, 0x77, 0x6b, 0xc5, 0x56, 0xbb, 0x99, 0xfa, 0xbd, 0xba,
	0x7c, 0x02, 0x30, 0x63, 0x05, 0x7c, 0xc8, 0x81, 0x37, 0xe8, 0x9e, 0x04, 0x67, 0x6a, 0xaa, 0xa3,
	0xfe, 0x74, 0xa9, 0xeb, 0xff, 0xd4, 0x94, 0xb2, 0xdd, 0x59, 0x28, 0x98, 0x71, 0x50, 0x6f, 0x71,
	0xed, 0x94, 0xeb, 0x89, 0x5e, 0xac, 0x36, 0xea, 0xc5, 0xd9, 0xa0, 0x1d, 0x7f, 0x7a, 0x2c, 0x5e,
	0xf6, 0xf3, 0xf5, 0xae, 0xbe, 0x03, 0x00, 0x00, 0xff, 0xff, 0x8a, 0x7c, 0x89, 0xea, 0x5c, 0x01,
	0x00, 0x00,
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

// SimServiceServer is the server API for SimService service.
type SimServiceServer interface {
	SimServiceGet(context.Context, *SimCommonFile) (*SimCacheStatus, error)
}

// UnimplementedSimServiceServer can be embedded to have forward compatible implementations.
type UnimplementedSimServiceServer struct {
}

func (*UnimplementedSimServiceServer) SimServiceGet(ctx context.Context, req *SimCommonFile) (*SimCacheStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method SimServiceGet not implemented")
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

var _SimService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "simservice.SimService",
	HandlerType: (*SimServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "SimServiceGet",
			Handler:    _SimService_SimServiceGet_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "simService/simService.proto",
}
