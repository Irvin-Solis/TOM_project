
### TODO: 
# Author name: Irvin Jimenez-Solis


from simulator import *
import random

# Multicast events for the driver
multicast_events = [
    (10, 'M1', 0, 'Do action ABC'),
    (20, 'M2', 1, 'Do action DEF'),
    (30, 'M3', 2, 'Do action IJK'),
    (40, 'M4', 0, 'Do action PQR'),
    (50, 'M5', 1, 'Do action XYZ'),
]


class Host(Node):
    def __init__(self, sim, host_id):
        Node.__init__(self, sim, host_id)
        self.host_id = host_id
        self.gmembers = []
    
    def initialize(self):
        # TODO: Initilize any data structure or state
        self.queue = {}
        self.seqNo = {}
        self.proposed = 0
        self.agreed = 0
        pass

    def multicast(self, time, message_id, message_type, payload):
        # Multicast message to the group
        print(f'Time {time}:: {self} SENDING mulitcast message [{message_id}]')

        for to in self.gmembers:
            mcast = Message(message_id, self, to, message_type, payload)
            self.send_message(to, mcast)

    def repyWithSeqNo(self, message):
        propSeqNo = max([self.agreed, self.proposed]) + 1
        self.proposed = self.proposed + 1

        if message.message_id in message.src.seqNo:
            message.src.seqNo[message.message_id].append(propSeqNo)
        else:
            message.src.seqNo[message.message_id] = [propSeqNo]

        self.queue[propSeqNo] = message
        # sort smallest => biggest
        dict(sorted(self.queue.items()))


    def handleReplies(self, message, time):
        maxSeqNo = max(message.src.seqNo[message.message_id])

        message.src.multicast(time, message.message_id, "Max_seqNo", maxSeqNo)
     
    def receive_message(self, time, frm, message):
        # This function is called when a message is received 
        # TODO: Currently simply delivers the received messages, does not implement any order
        # TODO: You need to implement your ordering code here

        if ("DRIVER_MCAST" == message.mtype):
            print(f'Time {time}:: {self} RECEIVED message [{message.message_id}] from {frm}')
            self.repyWithSeqNo(message)
            if message.message_id in message.src.seqNo:
                if (len(message.src.seqNo[message.message_id]) == len(self.gmembers)):
                    self.handleReplies(message, time)

        else:
            self.agreed = message.payload

            for propsedSeqNo, msg in self.queue.items():
                if msg.message_id == message.message_id:
                    del self.queue[propsedSeqNo]
                    self.queue[message.payload] = msg
                    break

            if self.proposed == message.payload:
                self.deliver_message(time, message)

            self.queue = dict(sorted(self.queue.items())) # sort our queue

            # if first message has a selected seqNo, deliver it
        if len(self.queue) > 0:
            if self.queue[list(self.queue)[0]] == self.agreed:
                self.deliver_message(time, message)
    
    def deliver_message(self, time, message):
        print(f'Time {time}:: {self} DELIVERED message [{message.message_id}]')
        

# Driver: you DO NOT need to change anything here
class Driver:
    def __init__(self, sim):
        self.hosts = []
        self.sim = sim

    def run(self, nhosts=3):
        for i in range(nhosts):
            host = Host(self.sim, i)
            self.hosts.append(host)
        
        for host in self.hosts:
            host.gmembers = self.hosts
            host.initialize()

        for event in multicast_events:
            time = event[0]
            message_id = event[1]
            message_type = 'DRIVER_MCAST'
            host_id = event[2]
            payload = event[3]
            self.sim.add_event(Event(time, self.hosts[host_id].multicast, time, message_id, message_type, payload))

def main():
    # Create simulation instance
    sim = Simulator(debug=False)

    # Start the driver and run for nhosts (should be >= 3)
    driver = Driver(sim)
    driver.run(nhosts=3)

    # Start the simulation
    sim.run()                 

if __name__ == "__main__":
    main()    
