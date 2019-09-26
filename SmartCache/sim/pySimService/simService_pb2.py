# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: simService/simService.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor.FileDescriptor(
  name='simService/simService.proto',
  package='simservice',
  syntax='proto3',
  serialized_options=_b('\n\031io.grpc.cache.sim.serviceB\017CacheSimServiceP\001'),
  serialized_pb=_b('\n\x1bsimService/simService.proto\x12\nsimservice\x1a\x1bgoogle/protobuf/empty.proto\"/\n\x0c\x41\x63tionResult\x12\x10\n\x08\x66ilename\x18\x01 \x01(\t\x12\r\n\x05\x61\x64\x64\x65\x64\x18\x02 \x01(\x08\"\x97\x01\n\x0eSimCacheStatus\x12\x0f\n\x07hitRate\x18\x01 \x01(\x02\x12\x17\n\x0fweightedHitRate\x18\x02 \x01(\x02\x12\x13\n\x0bhitOverMiss\x18\x03 \x01(\x02\x12\x0c\n\x04size\x18\x04 \x01(\x02\x12\x10\n\x08\x63\x61pacity\x18\x05 \x01(\x02\x12\x13\n\x0bwrittenData\x18\x06 \x01(\x02\x12\x11\n\treadOnHit\x18\x07 \x01(\x02\"/\n\rSimCommonFile\x12\x10\n\x08\x66ilename\x18\x01 \x01(\t\x12\x0c\n\x04size\x18\x02 \x01(\x02\"\x1c\n\rSimDumpRecord\x12\x0b\n\x03raw\x18\x01 \x01(\x0c\x32\xf9\x03\n\nSimService\x12?\n\x06SimGet\x12\x19.simservice.SimCommonFile\x1a\x18.simservice.ActionResult\"\x00\x12@\n\x08SimClear\x12\x16.google.protobuf.Empty\x1a\x1a.simservice.SimCacheStatus\"\x00\x12\x45\n\rSimClearFiles\x12\x16.google.protobuf.Empty\x1a\x1a.simservice.SimCacheStatus\"\x00\x12L\n\x14SimClearHitMissStats\x12\x16.google.protobuf.Empty\x1a\x1a.simservice.SimCacheStatus\"\x00\x12M\n\x15SimGetInfoCacheStatus\x12\x16.google.protobuf.Empty\x1a\x1a.simservice.SimCacheStatus\"\x00\x12\x41\n\x08SimDumps\x12\x16.google.protobuf.Empty\x1a\x19.simservice.SimDumpRecord\"\x00\x30\x01\x12\x41\n\x08SimLoads\x12\x19.simservice.SimDumpRecord\x1a\x16.google.protobuf.Empty\"\x00(\x01\x42.\n\x19io.grpc.cache.sim.serviceB\x0f\x43\x61\x63heSimServiceP\x01\x62\x06proto3')
  ,
  dependencies=[google_dot_protobuf_dot_empty__pb2.DESCRIPTOR,])




_ACTIONRESULT = _descriptor.Descriptor(
  name='ActionResult',
  full_name='simservice.ActionResult',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='filename', full_name='simservice.ActionResult.filename', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='added', full_name='simservice.ActionResult.added', index=1,
      number=2, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=72,
  serialized_end=119,
)


_SIMCACHESTATUS = _descriptor.Descriptor(
  name='SimCacheStatus',
  full_name='simservice.SimCacheStatus',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='hitRate', full_name='simservice.SimCacheStatus.hitRate', index=0,
      number=1, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='weightedHitRate', full_name='simservice.SimCacheStatus.weightedHitRate', index=1,
      number=2, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='hitOverMiss', full_name='simservice.SimCacheStatus.hitOverMiss', index=2,
      number=3, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='size', full_name='simservice.SimCacheStatus.size', index=3,
      number=4, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='capacity', full_name='simservice.SimCacheStatus.capacity', index=4,
      number=5, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='writtenData', full_name='simservice.SimCacheStatus.writtenData', index=5,
      number=6, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='readOnHit', full_name='simservice.SimCacheStatus.readOnHit', index=6,
      number=7, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=122,
  serialized_end=273,
)


_SIMCOMMONFILE = _descriptor.Descriptor(
  name='SimCommonFile',
  full_name='simservice.SimCommonFile',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='filename', full_name='simservice.SimCommonFile.filename', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='size', full_name='simservice.SimCommonFile.size', index=1,
      number=2, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=275,
  serialized_end=322,
)


_SIMDUMPRECORD = _descriptor.Descriptor(
  name='SimDumpRecord',
  full_name='simservice.SimDumpRecord',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='raw', full_name='simservice.SimDumpRecord.raw', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=_b(""),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=324,
  serialized_end=352,
)

DESCRIPTOR.message_types_by_name['ActionResult'] = _ACTIONRESULT
DESCRIPTOR.message_types_by_name['SimCacheStatus'] = _SIMCACHESTATUS
DESCRIPTOR.message_types_by_name['SimCommonFile'] = _SIMCOMMONFILE
DESCRIPTOR.message_types_by_name['SimDumpRecord'] = _SIMDUMPRECORD
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

ActionResult = _reflection.GeneratedProtocolMessageType('ActionResult', (_message.Message,), {
  'DESCRIPTOR' : _ACTIONRESULT,
  '__module__' : 'simService.simService_pb2'
  # @@protoc_insertion_point(class_scope:simservice.ActionResult)
  })
_sym_db.RegisterMessage(ActionResult)

SimCacheStatus = _reflection.GeneratedProtocolMessageType('SimCacheStatus', (_message.Message,), {
  'DESCRIPTOR' : _SIMCACHESTATUS,
  '__module__' : 'simService.simService_pb2'
  # @@protoc_insertion_point(class_scope:simservice.SimCacheStatus)
  })
_sym_db.RegisterMessage(SimCacheStatus)

SimCommonFile = _reflection.GeneratedProtocolMessageType('SimCommonFile', (_message.Message,), {
  'DESCRIPTOR' : _SIMCOMMONFILE,
  '__module__' : 'simService.simService_pb2'
  # @@protoc_insertion_point(class_scope:simservice.SimCommonFile)
  })
_sym_db.RegisterMessage(SimCommonFile)

SimDumpRecord = _reflection.GeneratedProtocolMessageType('SimDumpRecord', (_message.Message,), {
  'DESCRIPTOR' : _SIMDUMPRECORD,
  '__module__' : 'simService.simService_pb2'
  # @@protoc_insertion_point(class_scope:simservice.SimDumpRecord)
  })
_sym_db.RegisterMessage(SimDumpRecord)


DESCRIPTOR._options = None

_SIMSERVICE = _descriptor.ServiceDescriptor(
  name='SimService',
  full_name='simservice.SimService',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  serialized_start=355,
  serialized_end=860,
  methods=[
  _descriptor.MethodDescriptor(
    name='SimGet',
    full_name='simservice.SimService.SimGet',
    index=0,
    containing_service=None,
    input_type=_SIMCOMMONFILE,
    output_type=_ACTIONRESULT,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='SimClear',
    full_name='simservice.SimService.SimClear',
    index=1,
    containing_service=None,
    input_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    output_type=_SIMCACHESTATUS,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='SimClearFiles',
    full_name='simservice.SimService.SimClearFiles',
    index=2,
    containing_service=None,
    input_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    output_type=_SIMCACHESTATUS,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='SimClearHitMissStats',
    full_name='simservice.SimService.SimClearHitMissStats',
    index=3,
    containing_service=None,
    input_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    output_type=_SIMCACHESTATUS,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='SimGetInfoCacheStatus',
    full_name='simservice.SimService.SimGetInfoCacheStatus',
    index=4,
    containing_service=None,
    input_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    output_type=_SIMCACHESTATUS,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='SimDumps',
    full_name='simservice.SimService.SimDumps',
    index=5,
    containing_service=None,
    input_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    output_type=_SIMDUMPRECORD,
    serialized_options=None,
  ),
  _descriptor.MethodDescriptor(
    name='SimLoads',
    full_name='simservice.SimService.SimLoads',
    index=6,
    containing_service=None,
    input_type=_SIMDUMPRECORD,
    output_type=google_dot_protobuf_dot_empty__pb2._EMPTY,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_SIMSERVICE)

DESCRIPTOR.services_by_name['SimService'] = _SIMSERVICE

# @@protoc_insertion_point(module_scope)
