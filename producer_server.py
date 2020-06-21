from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    #TODO we're generating a dummy data
    def generate_data(self):
        #print(self.topic)
        #print(self.input_file)
        with open(self.input_file, 'r') as f:
            data=json.load(f)
            for line in data:
                #print(line)
                message = self.dict_to_binary(line)
                # TODO send the correct data
                #print(message);
                self.send(self.topic, message)
                time.sleep(1)

    # TODO fill this in to return the json dictionary to binary
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')
        