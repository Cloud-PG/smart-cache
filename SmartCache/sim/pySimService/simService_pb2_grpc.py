# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from . import simService_pb2 as simService_dot_simService__pb2


class SimServiceStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.SimGet = channel.unary_unary(
        '/simservice.SimService/SimGet',
        request_serializer=simService_dot_simService__pb2.SimCommonFile.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.ActionResult.FromString,
        )
    self.SimReset = channel.unary_unary(
        '/simservice.SimService/SimReset',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.SimCacheStatus.FromString,
        )
    self.SimGetInfoCacheStatus = channel.unary_unary(
        '/simservice.SimService/SimGetInfoCacheStatus',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.SimCacheStatus.FromString,
        )
    self.SimGetInfoCacheFiles = channel.unary_stream(
        '/simservice.SimService/SimGetInfoCacheFiles',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.SimCommonFile.FromString,
        )
    self.SimGetInfoFilesWeights = channel.unary_stream(
        '/simservice.SimService/SimGetInfoFilesWeights',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.SimFileWeight.FromString,
        )
    self.SimGetInfoFilesStats = channel.unary_stream(
        '/simservice.SimService/SimGetInfoFilesStats',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.SimFileStats.FromString,
        )


class SimServiceServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def SimGet(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SimReset(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SimGetInfoCacheStatus(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SimGetInfoCacheFiles(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SimGetInfoFilesWeights(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SimGetInfoFilesStats(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_SimServiceServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'SimGet': grpc.unary_unary_rpc_method_handler(
          servicer.SimGet,
          request_deserializer=simService_dot_simService__pb2.SimCommonFile.FromString,
          response_serializer=simService_dot_simService__pb2.ActionResult.SerializeToString,
      ),
      'SimReset': grpc.unary_unary_rpc_method_handler(
          servicer.SimReset,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=simService_dot_simService__pb2.SimCacheStatus.SerializeToString,
      ),
      'SimGetInfoCacheStatus': grpc.unary_unary_rpc_method_handler(
          servicer.SimGetInfoCacheStatus,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=simService_dot_simService__pb2.SimCacheStatus.SerializeToString,
      ),
      'SimGetInfoCacheFiles': grpc.unary_stream_rpc_method_handler(
          servicer.SimGetInfoCacheFiles,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=simService_dot_simService__pb2.SimCommonFile.SerializeToString,
      ),
      'SimGetInfoFilesWeights': grpc.unary_stream_rpc_method_handler(
          servicer.SimGetInfoFilesWeights,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=simService_dot_simService__pb2.SimFileWeight.SerializeToString,
      ),
      'SimGetInfoFilesStats': grpc.unary_stream_rpc_method_handler(
          servicer.SimGetInfoFilesStats,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=simService_dot_simService__pb2.SimFileStats.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'simservice.SimService', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
