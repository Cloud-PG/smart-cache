# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: ai.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='ai.proto',
  package='aiservice',
  syntax='proto3',
  serialized_options=_b('\n\030io.grpc.cache.ai.serviceB\016CacheAIServiceP\001'),
  serialized_pb=_b('\n\x08\x61i.proto\x12\taiservice\"\x90\x01\n\x07\x41IInput\x12\x10\n\x08\x66ilename\x18\x01 \x01(\t\x12\x10\n\x08siteName\x18\x02 \x01(\t\x12\x10\n\x08\x64\x61taType\x18\x03 \x01(\t\x12\x10\n\x08\x66ileType\x18\x04 \x01(\t\x12\x0e\n\x06userID\x18\x05 \x01(\r\x12\x0e\n\x06numReq\x18\x06 \x01(\r\x12\x0f\n\x07\x61vgTime\x18\x07 \x01(\x02\x12\x0c\n\x04size\x18\x08 \x01(\x02\" \n\x0fStorePrediction\x12\r\n\x05store\x18\x01 \x01(\x08\x32M\n\tAIService\x12@\n\x0c\x41IPredictOne\x12\x12.aiservice.AIInput\x1a\x1a.aiservice.StorePrediction\"\x00\x42,\n\x18io.grpc.cache.ai.serviceB\x0e\x43\x61\x63heAIServiceP\x01\x62\x06proto3')
)




_AIINPUT = _descriptor.Descriptor(
  name='AIInput',
  full_name='aiservice.AIInput',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='filename', full_name='aiservice.AIInput.filename', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='siteName', full_name='aiservice.AIInput.siteName', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='dataType', full_name='aiservice.AIInput.dataType', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='fileType', full_name='aiservice.AIInput.fileType', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='userID', full_name='aiservice.AIInput.userID', index=4,
      number=5, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='numReq', full_name='aiservice.AIInput.numReq', index=5,
      number=6, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='avgTime', full_name='aiservice.AIInput.avgTime', index=6,
      number=7, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='size', full_name='aiservice.AIInput.size', index=7,
      number=8, type=2, cpp_type=6, label=1,
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
  serialized_start=24,
  serialized_end=168,
)


_STOREPREDICTION = _descriptor.Descriptor(
  name='StorePrediction',
  full_name='aiservice.StorePrediction',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='store', full_name='aiservice.StorePrediction.store', index=0,
      number=1, type=8, cpp_type=7, label=1,
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
  serialized_start=170,
  serialized_end=202,
)

DESCRIPTOR.message_types_by_name['AIInput'] = _AIINPUT
DESCRIPTOR.message_types_by_name['StorePrediction'] = _STOREPREDICTION
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

AIInput = _reflection.GeneratedProtocolMessageType('AIInput', (_message.Message,), {
  'DESCRIPTOR' : _AIINPUT,
  '__module__' : 'ai_pb2'
  # @@protoc_insertion_point(class_scope:aiservice.AIInput)
  })
_sym_db.RegisterMessage(AIInput)

StorePrediction = _reflection.GeneratedProtocolMessageType('StorePrediction', (_message.Message,), {
  'DESCRIPTOR' : _STOREPREDICTION,
  '__module__' : 'ai_pb2'
  # @@protoc_insertion_point(class_scope:aiservice.StorePrediction)
  })
_sym_db.RegisterMessage(StorePrediction)


DESCRIPTOR._options = None

_AISERVICE = _descriptor.ServiceDescriptor(
  name='AIService',
  full_name='aiservice.AIService',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  serialized_start=204,
  serialized_end=281,
  methods=[
  _descriptor.MethodDescriptor(
    name='AIPredictOne',
    full_name='aiservice.AIService.AIPredictOne',
    index=0,
    containing_service=None,
    input_type=_AIINPUT,
    output_type=_STOREPREDICTION,
    serialized_options=None,
  ),
])
_sym_db.RegisterServiceDescriptor(_AISERVICE)

DESCRIPTOR.services_by_name['AIService'] = _AISERVICE

# @@protoc_insertion_point(module_scope)
