# Coded by Mark Kiefer, Nancy Laseko, and Evan Buckspan
# CS 4459B - Project
# Due by April 4, 2025

import grpc
import replication_pb2
import replication_pb2_grpc
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
from concurrent import futures
import time
import threading

class SequenceServicer(replication_pb2_grpc.SequenceServicer):

    def __init__(self):

        # dictionary
        self.data = {}
        self.is_primary = False

    # function to send write request to backups, receive ack, apply write, and send ack
    def Write(self, request, context):

        # get metadata for information on sender, (client, server, heartbeat)
        metadata = dict(context.invocation_metadata())
        source = metadata.get("source", "unknown")  

        if source == "client" and self.is_primary:
            pass

        elif source == "client" and not self.is_primary:
            return replication_pb2.WriteResponse(ack="Nack")
        
        elif source == "heartbeat":
            self.is_primary = True

            return replication_pb2.WriteResponse(ack="ack")

        if self.is_primary:

            try:
            
                # connect to server 2
                with grpc.insecure_channel('localhost:50052') as channel:

                    # create stub
                    stub = replication_pb2_grpc.SequenceStub(channel)

                    # call write request
                    metadata = (("source", "server"),)
                    response_2 = stub.Write(request, metadata=metadata)
        
            # server not up
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("Error: Server 2 is unavailable")

                else:
                    print(f"Error: {e.code()}")

            try:

                # connect to server 3
                with grpc.insecure_channel('localhost:50053') as channel:

                    # create stub
                    stub = replication_pb2_grpc.SequenceStub(channel)

                    # call write request
                    metadata = (("source", "server"),)
                    response_3 = stub.Write(request, metadata=metadata)
        
            # server not up
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("Error: Server 3 is unavailable")

                else:
                    print(f"Error: {e.code()}")

            try:

                # connect to server 4
                with grpc.insecure_channel('localhost:50054') as channel:

                    # create stub
                    stub = replication_pb2_grpc.SequenceStub(channel)

                    # call write request
                    metadata = (("source", "server"),)
                    response_4 = stub.Write(request, metadata=metadata)
        
            # server not up
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    print("Error: Server 4 is unavailable")

                else:
                    print(f"Error: {e.code()}")

            # count number of acks received
            ack_count = 0

            try:
                if response_2.ack == "ack":
                    ack_count += 1
            except:
                pass

            try:
                if response_3.ack == "ack":
                    ack_count += 1
            except:
                pass
            
            try:
                if response_4.ack == "ack":
                    ack_count += 1
            except:
                pass

            # must receive at least two acks to send ack back to client
            if ack_count >= 2:

                # apply write
                self.data[request.key] = request.value

                # update log
                with open("server_1.txt", 'a') as file:
                    file.write(f"{request.key} {request.value}\n")

                # return ack
                return replication_pb2.WriteResponse(ack="ack")
            
            else:
                context.set_code(grpc.StatusCode.UNAVAILABLE)
                return replication_pb2.WriteResponse(ack="Nack")
        

        if not self.is_primary:

            # apply write
            self.data[request.key] = request.value

            # update log
            with open("server_1.txt", 'a') as file:
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
                message = "server_1"
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
    server.add_insecure_port("[::]:50051")

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