// Code generated by protoc-gen-go. DO NOT EDIT.
// source: simService/simService.proto

/*
Package simservice is a generated protocol buffer package.

It is generated from these files:
	simService/simService.proto

It has these top-level messages:
	SimCommonFile
	SimCacheStatus
	SimCacheInfo
*/
package simservice

import (
	fmt "fmt"

	proto "github.com/golang/protobuf/proto"

	math "math"

	google_protobuf "github.com/golang/protobuf/ptypes/empty"

	context "golang.org/x/net/context"

	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type SimCommonFile struct {
	Filename string  `protobuf:"bytes,1,opt,name=filename" json:"filename,omitempty"`
	Size     float32 `protobuf:"fixed32,2,opt,name=size" json:"size,omitempty"`
}

func (m *SimCommonFile) Reset()                    { *m = SimCommonFile{} }
func (m *SimCommonFile) String() string            { return proto.CompactTextString(m) }
func (*SimCommonFile) ProtoMessage()               {}
func (*SimCommonFile) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

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
	HitRate     float32 `protobuf:"fixed32,1,opt,name=hitRate" json:"hitRate,omitempty"`
	Size        float32 `protobuf:"fixed32,2,opt,name=size" json:"size,omitempty"`
	WrittenData float32 `protobuf:"fixed32,3,opt,name=writtenData" json:"writtenData,omitempty"`
	Capacity    float32 `protobuf:"fixed32,4,opt,name=capacity" json:"capacity,omitempty"`
}

func (m *SimCacheStatus) Reset()                    { *m = SimCacheStatus{} }
func (m *SimCacheStatus) String() string            { return proto.CompactTextString(m) }
func (*SimCacheStatus) ProtoMessage()               {}
func (*SimCacheStatus) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{1} }

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

type SimCacheInfo struct {
	CacheFiles  map[string]float32 `protobuf:"bytes,1,rep,name=cacheFiles" json:"cacheFiles,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"fixed32,2,opt,name=value"`
	FileWeights map[string]float32 `protobuf:"bytes,2,rep,name=fileWeights" json:"fileWeights,omitempty" protobuf_key:"bytes,1,opt,name=key" protobuf_val:"fixed32,2,opt,name=value"`
}

func (m *SimCacheInfo) Reset()                    { *m = SimCacheInfo{} }
func (m *SimCacheInfo) String() string            { return proto.CompactTextString(m) }
func (*SimCacheInfo) ProtoMessage()               {}
func (*SimCacheInfo) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{2} }

func (m *SimCacheInfo) GetCacheFiles() map[string]float32 {
	if m != nil {
		return m.CacheFiles
	}
	return nil
}

func (m *SimCacheInfo) GetFileWeights() map[string]float32 {
	if m != nil {
		return m.FileWeights
	}
	return nil
}

func init() {
	proto.RegisterType((*SimCommonFile)(nil), "simservice.SimCommonFile")
	proto.RegisterType((*SimCacheStatus)(nil), "simservice.SimCacheStatus")
	proto.RegisterType((*SimCacheInfo)(nil), "simservice.SimCacheInfo")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// Client API for SimService service

type SimServiceClient interface {
	SimServiceGet(ctx context.Context, in *SimCommonFile, opts ...grpc.CallOption) (*SimCacheStatus, error)
	SimServiceClear(ctx context.Context, in *google_protobuf.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error)
	SimServiceInfo(ctx context.Context, in *google_protobuf.Empty, opts ...grpc.CallOption) (*SimCacheInfo, error)
}

type simServiceClient struct {
	cc *grpc.ClientConn
}

func NewSimServiceClient(cc *grpc.ClientConn) SimServiceClient {
	return &simServiceClient{cc}
}

func (c *simServiceClient) SimServiceGet(ctx context.Context, in *SimCommonFile, opts ...grpc.CallOption) (*SimCacheStatus, error) {
	out := new(SimCacheStatus)
	err := grpc.Invoke(ctx, "/simservice.SimService/SimServiceGet", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simServiceClient) SimServiceClear(ctx context.Context, in *google_protobuf.Empty, opts ...grpc.CallOption) (*SimCacheStatus, error) {
	out := new(SimCacheStatus)
	err := grpc.Invoke(ctx, "/simservice.SimService/SimServiceClear", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *simServiceClient) SimServiceInfo(ctx context.Context, in *google_protobuf.Empty, opts ...grpc.CallOption) (*SimCacheInfo, error) {
	out := new(SimCacheInfo)
	err := grpc.Invoke(ctx, "/simservice.SimService/SimServiceInfo", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for SimService service

type SimServiceServer interface {
	SimServiceGet(context.Context, *SimCommonFile) (*SimCacheStatus, error)
	SimServiceClear(context.Context, *google_protobuf.Empty) (*SimCacheStatus, error)
	SimServiceInfo(context.Context, *google_protobuf.Empty) (*SimCacheInfo, error)
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
	in := new(google_protobuf.Empty)
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
		return srv.(SimServiceServer).SimServiceClear(ctx, req.(*google_protobuf.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

func _SimService_SimServiceInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(google_protobuf.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(SimServiceServer).SimServiceInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/simservice.SimService/SimServiceInfo",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(SimServiceServer).SimServiceInfo(ctx, req.(*google_protobuf.Empty))
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
		{
			MethodName: "SimServiceClear",
			Handler:    _SimService_SimServiceClear_Handler,
		},
		{
			MethodName: "SimServiceInfo",
			Handler:    _SimService_SimServiceInfo_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "simService/simService.proto",
}

func init() { proto.RegisterFile("simService/simService.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 400 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x52, 0xcd, 0xca, 0xd3, 0x40,
	0x14, 0xfd, 0x92, 0xd6, 0xbf, 0x5b, 0xb5, 0x65, 0x10, 0x99, 0xa6, 0x9b, 0x90, 0x55, 0xdc, 0x4c,
	0xa1, 0x6e, 0x44, 0x50, 0xa1, 0xff, 0xe2, 0x46, 0x92, 0x85, 0xeb, 0x69, 0x98, 0xa6, 0x83, 0x49,
	0x26, 0x24, 0xd3, 0x4a, 0xc4, 0xc7, 0xf1, 0xd5, 0x7c, 0x0f, 0x99, 0x99, 0xfc, 0x51, 0x5a, 0xc4,
	0xdd, 0x9c, 0xdc, 0x73, 0xcf, 0xcd, 0xbd, 0xe7, 0xc0, 0xac, 0xe4, 0x69, 0xc8, 0x8a, 0x0b, 0x8f,
	0xd8, 0xbc, 0x7b, 0x92, 0xbc, 0x10, 0x52, 0x20, 0x28, 0x79, 0x5a, 0x9a, 0x2f, 0xce, 0x2c, 0x16,
	0x22, 0x4e, 0xd8, 0x5c, 0x57, 0x0e, 0xe7, 0xe3, 0x9c, 0xa5, 0xb9, 0xac, 0x0c, 0xd1, 0xfb, 0x04,
	0x2f, 0x42, 0x9e, 0xae, 0x44, 0x9a, 0x8a, 0x6c, 0xcb, 0x13, 0x86, 0x1c, 0x78, 0x7a, 0xe4, 0x09,
	0xcb, 0x68, 0xca, 0xb0, 0xe5, 0x5a, 0xfe, 0xb3, 0xa0, 0xc5, 0x08, 0xc1, 0xb0, 0xe4, 0x3f, 0x19,
	0xb6, 0x5d, 0xcb, 0xb7, 0x03, 0xfd, 0xf6, 0x7e, 0xc1, 0x4b, 0x25, 0x40, 0xa3, 0x13, 0x0b, 0x25,
	0x95, 0xe7, 0x12, 0x61, 0x78, 0x72, 0xe2, 0x32, 0xa0, 0xd2, 0x08, 0xd8, 0x41, 0x03, 0x6f, 0xf5,
	0x23, 0x17, 0x46, 0x3f, 0x0a, 0x2e, 0x25, 0xcb, 0xd6, 0x54, 0x52, 0x3c, 0xd0, 0xa5, 0xfe, 0x27,
	0xf5, 0x47, 0x11, 0xcd, 0x69, 0xc4, 0x65, 0x85, 0x87, 0xba, 0xdc, 0x62, 0xef, 0xb7, 0x0d, 0xcf,
	0x9b, 0xf1, 0x9f, 0xb3, 0xa3, 0x40, 0x7b, 0x80, 0x48, 0x01, 0xb5, 0x4b, 0x89, 0x2d, 0x77, 0xe0,
	0x8f, 0x16, 0x3e, 0xe9, 0xae, 0x41, 0xfa, 0x6c, 0xb2, 0x6a, 0xa9, 0x9b, 0x4c, 0x16, 0x55, 0xd0,
	0xeb, 0x45, 0x5f, 0x60, 0xa4, 0x16, 0xff, 0xc6, 0x78, 0x7c, 0x92, 0x25, 0xb6, 0xb5, 0xd4, 0x9b,
	0xbb, 0x52, 0xdb, 0x8e, 0x6b, 0xb4, 0xfa, 0xdd, 0xce, 0x07, 0x18, 0x5f, 0xcd, 0x42, 0x13, 0x18,
	0x7c, 0x67, 0x55, 0x7d, 0x63, 0xf5, 0x44, 0xaf, 0xe0, 0xd1, 0x85, 0x26, 0xe7, 0xe6, 0x3e, 0x06,
	0xbc, 0xb7, 0xdf, 0x59, 0xce, 0x47, 0x98, 0x5c, 0xeb, 0xff, 0x4f, 0xff, 0xe2, 0x8f, 0x05, 0x10,
	0xb6, 0x19, 0x41, 0x7b, 0x6d, 0x7a, 0x8d, 0x76, 0x4c, 0xa2, 0xe9, 0xf5, 0x5a, 0x6d, 0x1e, 0x1c,
	0xe7, 0xd6, 0xc6, 0xc6, 0x69, 0xef, 0x01, 0xed, 0x60, 0xdc, 0x29, 0xad, 0x12, 0x46, 0x0b, 0xf4,
	0x9a, 0x98, 0xbc, 0x91, 0x26, 0x6f, 0x64, 0xa3, 0xf2, 0xf6, 0x0f, 0xa1, 0xb5, 0x8e, 0x51, 0x2d,
	0xa4, 0x9d, 0xbc, 0xa7, 0x83, 0xef, 0x59, 0xe0, 0x3d, 0x2c, 0x09, 0x4c, 0xb9, 0x20, 0x71, 0x91,
	0x47, 0x44, 0x3b, 0xa9, 0xa8, 0xa4, 0xe6, 0x2e, 0x8d, 0x03, 0xdd, 0x94, 0xaf, 0xd6, 0xe1, 0xb1,
	0xd6, 0x7e, 0xfb, 0x37, 0x00, 0x00, 0xff, 0xff, 0xa9, 0x7a, 0x85, 0x23, 0x4c, 0x03, 0x00, 0x00,
}
