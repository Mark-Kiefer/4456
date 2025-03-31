# Coded by Mark Kiefer (251237385)
# CS 4459B - Assignment 2
# Due by March 11, 2025

import grpc
import replication_pb2
import replication_pb2_grpc
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
from concurrent import futures
import time
import threading

is_primary = False


class SequenceServicer(replication_pb2_grpc.SequenceServicer):

    def __init__(self):

        # dictionary
        self.data = {}
        self.is_primary = False

    # function to send write request to backups, receive ack, apply write, and send ack
    def Write(self, request, context):

        if self.is_primary:

            try:

                # connect to server 1
                with grpc.insecure_channel('localhost:50051') as channel:

                    # create stub
                    stub = replication_pb2_grpc.SequenceStub(channel)

                    # call write request
                    response_1 = stub.Write(request)
        
            # server not up
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("Error: Server 1 is unavailable")
                    context.set_code(grpc.StatusCode.UNAVAILABLE)

                else:
                    print(f"Error: {e.code()}")
                    context.set_code(e.code())
                    context.set_details(e.details())

            try:

                # connect to server 2
                with grpc.insecure_channel('localhost:50052') as channel:

                    # create stub
                    stub = replication_pb2_grpc.SequenceStub(channel)

                    # call write request
                    response_2 = stub.Write(request)
        
            # server not up
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("Error: Server 2 is unavailable")
                    context.set_code(grpc.StatusCode.UNAVAILABLE)

                else:
                    print(f"Error: {e.code()}")
                    context.set_code(e.code())
                    context.set_details(e.details())

            try:

                # connect to server 3
                with grpc.insecure_channel('localhost:50053') as channel:

                    # create stub
                    stub = replication_pb2_grpc.SequenceStub(channel)

                    # call write request
                    response_3 = stub.Write(request)
        
            # server not up
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("Error: Server 3 is unavailable")
                    context.set_code(grpc.StatusCode.UNAVAILABLE)

                else:
                    print(f"Error: {e.code()}")
                    context.set_code(e.code())
                    context.set_details(e.details())

            # received ack
            if (response_2.ack == "ack" and response_3.ack == "ack") or (response_2.ack == "ack" and response_1.ack == "ack") or (response_3.ack == "ack" and response_1.ack == "ack"):

                # apply write
                self.data[request.key] = request.value

                # update log
                with open("server_4.txt", 'a') as file:
                    file.write(f"{request.key} {request.value}\n")

                # return ack
                return replication_pb2.WriteResponse(ack="ack")

        if not self.is_primary:

            # apply write
            self.data[request.key] = request.value

            # update log
            with open("server_4.txt", 'a') as file:
                file.write(f"{request.key} {request.value}\n")

            # return ack
            return replication_pb2.WriteResponse(ack="ack")


# function for sending heartbeats
def send_heartbeat():

    # send message to heartbeat server every 5 seconds
    while True:

        try:
            with grpc.insecure_channel('localhost:50056') as channel:

                # create stub
                stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)

                # prepare heartbeat
                message = "server_4"
                request = heartbeat_service_pb2.HeartbeatRequest(service_identifier=message)

                # call write request
                stub.Heartbeat(request)

        # server not up
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                print("Error: Heartbeat server is unavailable")

            else:
                print(f"Error: {e.code()}")

        # wait before sending another
        time.sleep(5)


# create server
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_SequenceServicer_to_server(SequenceServicer(), server)
    server.add_insecure_port("[::]:50054")

    try:
        print("Server started")
        server.start()
        
        heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
        heartbeat_thread.start()

        server.wait_for_termination()

    except KeyboardInterrupt:
        print("\nServer shutting down")
        server.stop(0)


if __name__ == "__main__":
    serve()