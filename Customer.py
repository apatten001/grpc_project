import grpc
from Branch_pb2 import MsgRequest
import Branch_pb2_grpc
from time import sleep


class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = None

    # TODO: students are expected to create the Customer stub
    def createStub(self):

        port = str(50000 + self.id)
        channel = grpc.insecure_channel("localhost:" + port)
        self.stub = Branch_pb2_grpc.BranchStub(channel)

    # TODO: students are expected to send out the events to the Bank
    def executeEvents(self):
        for event in self.events:
            # Sleep 3 seconds for 'query' events
            if event["interface"] == "query":
                sleep(3)

        # send request to Branch server
            response = self.stub.MsgDelivery(
              MsgRequest(id=event["id"], interface=event["interface"], money=event["money"])
            )

        # create msg to be appended to self.recvMsg list
            msg = {"interface": response.interface, "result": response.result}

            # add 'money' from entry for 'query' events
            if response.interface == "query":
                msg["money"] = response.money

            self.recvMsg.append(msg)

    # generation of an output msg
    def output(self):
        return {"id": self.id, "recv": self.recvMsg}
