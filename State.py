from datetime import datetime
import json

class WriteObj:
    def __init__(self, measurement, start_dt, end_dt, bucket_name, time_format):
        self.measurement = measurement
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.bucket_name = bucket_name
        self.time_format = time_format

    def __repr__(self):
        return f"WriteObj(measurement='{self.measurement}', start_dt='{self.start_dt}', end_dt='{self.end_dt}', bucket_name='{self.bucket_name}', time_format='{self.time_format}')"
    
    def to_dict(self):
        return {
            'measurement': self.measurement,
            'start_dt': self.start_dt.strftime(self.time_format),
            'end_dt': self.end_dt.strftime(self.time_format),
            'bucket_name': self.bucket_name,
            'time_format': self.time_format
        }
    
def save_writeobj_to_file(writeObjs, filename='writeobj.json'):
    with open(filename, 'w') as fileOperationWrite:
        json.dump([writeObj.to_dict() for writeObj in writeObjs], fileOperationWrite, indent=4)

def load_writeobj_from_file(filename='writeobj.json'):
    try:
        with open(filename, 'r') as fileOperationRead:
            writeobj_data = json.load(fileOperationRead)
            return [WriteObj(
                measurement=writeobj['measurement'],
                start_dt=writeobj['start_dt'],
                end_dt=writeobj['end_dt'],
                bucket_name=writeobj['bucket_name'],
                time_format=writeobj['time_format']
            ) for writeobj in writeobj_data]
    except FileNotFoundError:
        return []

def add_writeobj(measurement, start_dt, end_dt, bucket_name, time_format):
    new_writeObj = WriteObj(measurement, start_dt, end_dt, bucket_name, time_format)
    writeObjs.append(new_writeObj)
    save_writeobj_to_file(writeObjs)

def pop_writeobj():
    if writeObjs:
        removed_writeObj = writeObjs.pop()
        save_writeobj_to_file(writeObjs)
        return removed_writeObj
    else:
        return None

writeObjs = load_writeobj_from_file()
