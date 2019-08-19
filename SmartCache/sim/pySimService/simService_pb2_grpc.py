# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from simService import simService_pb2 as simService_dot_simService__pb2


class SimServiceStub(object):
  # missing associated documentation comment in .proto file
  pass

  def __init__(self, channel):
    """Constructor.

    Args:
      channel: A grpc.Channel.
    """
    self.SimServiceGet = channel.unary_unary(
        '/simservice.SimService/SimServiceGet',
        request_serializer=simService_dot_simService__pb2.SimCommonFile.SerializeToString,
        response_deserializer=simService_dot_simService__pb2.SimCacheStatus.FromString,
        )


class SimServiceServicer(object):
  # missing associated documentation comment in .proto file
  pass

  def SimServiceGet(self, request, context):
    # missing associated documentation comment in .proto file
    pass
    context.set_code(grpc.StatusCode.UNIMPLEMENTED)
    context.set_details('Method not implemented!')
    raise NotImplementedError('Method not implemented!')


def add_SimServiceServicer_to_server(servicer, server):
  rpc_method_handlers = {
      'SimServiceGet': grpc.unary_unary_rpc_method_handler(
          servicer.SimServiceGet,
          request_deserializer=simService_dot_simService__pb2.SimCommonFile.FromString,
          response_serializer=simService_dot_simService__pb2.SimCacheStatus.SerializeToString,
      ),
  }
  generic_handler = grpc.method_handlers_generic_handler(
      'simservice.SimService', rpc_method_handlers)
  server.add_generic_rpc_handlers((generic_handler,))
