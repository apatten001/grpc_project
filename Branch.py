
import grpc
import Branch_pb2_grpc
from Branch_pb2 import MsgRequest, MsgResponse


class Branch(Branch_pb2_grpc.BranchServicer):

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # iterate the processID of the branches

        # setup channel & client stub for each branch using grpc
    def createStubs(self):
        for branch_id in self.branches:
            if branch_id != self.id:
                port = str(50000 + branch_id)
                channel = grpc.insecure_channel("localhost:" + port)
                self.stubList.append(Branch_pb2_grpc.BranchStub(channel))

    # receives incoming message request from the customer transaction and starts message processing
    def MsgDelivery(self, request, context):
        return self.ProcessMsg(request, True)

    # allows for the Branch propagation from incoming msg request
    def MsgPropagation(self, request, context):
        return self.ProcessMsg(request, False)

    # handle received Msg, generate and return a MsgResponse
    def ProcessMsg(self, request, propagate):
        result = "success"

        if request.money < 0:
            result = "fail"
        elif request.interface == "query":
            pass
        elif request.interface == "deposit":
            self.balance += request.money
            if propagate:
                self.Propagate_Deposit(request)
        elif request.interface == "withdraw":
            if self.balance >= request.money:
                self.balance -= request.money
                if propagate:
                    self.Propagate_Withdraw(request)
            else:
                result = "fail"
        else:
            result = "fail"

        # Create msg to be appended to self.recvMsg list
        msg = {"interface": request.interface, "result": result}

        # Add 'money' entry for 'query' events
        if request.interface == "query":
            msg["money"] = request.money

        self.recvMsg.append(msg)

        return MsgResponse(interface=request.interface, result=result, money=self.balance)

    # Propagate Customer withdraw to other Branches
    def Propagate_Withdraw(self, request):
        for stub in self.stubList:
            stub.MsgPropagation(MsgRequest(id=request.id, interface="withdraw", money=request.money))

    # Propagate Customer deposit to other Branches
    def Propagate_Deposit(self, request):
        for stub in self.stubList:
            stub.MsgPropagation(MsgRequest(id=request.id, interface="deposit", money=request.money))




