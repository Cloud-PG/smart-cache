# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from . import pluginService_pb2 as pluginProto_dot_pluginService__pb2


class PluginServiceStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.GetHint = channel.unary_unary(
        '/pluginservice.PluginService/GetHint',
        request_serializer=pluginProto_dot_pluginService__pb2.FileRequest.SerializeToString,
        response_deserializer=pluginProto_dot_pluginService__pb2.FileHint.FromString,
        )
    self.UpdateStats = channel.unary_unary(
        '/pluginservice.PluginService/UpdateStats',
        request_serializer=pluginProto_dot_pluginService__pb2.FileRequest.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )
    self.ResetHistory = channel.unary_unary(
        '/pluginservice.PluginService/ResetHistory',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )


class PluginServiceServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def GetHint(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def UpdateStats(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def ResetHistory(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_PluginServiceServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'GetHint': grpc.unary_unary_rpc_method_handler(
          servicer.GetHint,
          request_deserializer=pluginProto_dot_pluginService__pb2.FileRequest.FromString,
          response_serializer=pluginProto_dot_pluginService__pb2.FileHint.SerializeToString,
      ),
      'UpdateStats': grpc.unary_unary_rpc_method_handler(
          servicer.UpdateStats,
          request_deserializer=pluginProto_dot_pluginService__pb2.FileRequest.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
      'ResetHistory': grpc.unary_unary_rpc_method_handler(
          servicer.ResetHistory,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'pluginservice.PluginService', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
