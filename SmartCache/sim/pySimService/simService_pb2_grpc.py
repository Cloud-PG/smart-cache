# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2
from . import simService_pb2 as simService_dot_simService__pb2


class SimServiceStub(object):
  """Service to interact with the simulated cache
  """

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
    self.SimClear = channel.unary_unary(
        '/simservice.SimService/SimClear',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.SimCacheStatus.FromString,
        )
    self.SimClearFiles = channel.unary_unary(
        '/simservice.SimService/SimClearFiles',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.SimCacheStatus.FromString,
        )
    self.SimClearHitMissStats = channel.unary_unary(
        '/simservice.SimService/SimClearHitMissStats',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.SimCacheStatus.FromString,
        )
    self.SimGetInfoCacheStatus = channel.unary_unary(
        '/simservice.SimService/SimGetInfoCacheStatus',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.SimCacheStatus.FromString,
        )
    self.SimDumps = channel.unary_stream(
        '/simservice.SimService/SimDumps',
        request_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.SimDumpRecord.FromString,
        )
    self.SimLoads = channel.stream_unary(
        '/simservice.SimService/SimLoads',
        request_serializer=simService_dot_simService__pb2.SimDumpRecord.SerializeToString,
        response_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
        )


class SimServiceServicer(object):
  """Service to interact with the simulated cache
  """

  def SimGet(self, request, context):
    """Requeste a file to the simulated cache
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SimClear(self, request, context):
    """Clear the cache, files, statistics and so on.
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SimClearFiles(self, request, context):
    """Clear only the files in the cache
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SimClearHitMissStats(self, request, context):
    """Reset only the statistics of the simulated cache
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SimGetInfoCacheStatus(self, request, context):
    """Retrieve the simulated cache status
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SimDumps(self, request, context):
    """Save the state of the current simulated cache
    """
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')

  def SimLoads(self, request_iterator, context):
    """Load a previuos saved state of the cache
    """
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
      'SimClear': grpc.unary_unary_rpc_method_handler(
          servicer.SimClear,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=simService_dot_simService__pb2.SimCacheStatus.SerializeToString,
      ),
      'SimClearFiles': grpc.unary_unary_rpc_method_handler(
          servicer.SimClearFiles,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=simService_dot_simService__pb2.SimCacheStatus.SerializeToString,
      ),
      'SimClearHitMissStats': grpc.unary_unary_rpc_method_handler(
          servicer.SimClearHitMissStats,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=simService_dot_simService__pb2.SimCacheStatus.SerializeToString,
      ),
      'SimGetInfoCacheStatus': grpc.unary_unary_rpc_method_handler(
          servicer.SimGetInfoCacheStatus,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=simService_dot_simService__pb2.SimCacheStatus.SerializeToString,
      ),
      'SimDumps': grpc.unary_stream_rpc_method_handler(
          servicer.SimDumps,
          request_deserializer=google_dot_protobuf_dot_empty__pb2.Empty.FromString,
          response_serializer=simService_dot_simService__pb2.SimDumpRecord.SerializeToString,
      ),
      'SimLoads': grpc.stream_unary_rpc_method_handler(
          servicer.SimLoads,
          request_deserializer=simService_dot_simService__pb2.SimDumpRecord.FromString,
          response_serializer=google_dot_protobuf_dot_empty__pb2.Empty.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'simservice.SimService', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
