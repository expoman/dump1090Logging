#!/usr/bin/python3

import urllib.request
import json
from influxdb import InfluxDBClient
import datetime

class dump1090Reader:
    def __init__(self, host):
        self.host = host
        self.url = 'http://'+host+'/dump1090/data/stats.json'
        print(self.url)

    def getData(self):
        req = urllib.request.Request(self.url)
        r = urllib.request.urlopen(req).read()
        cont = json.loads(r.decode('utf-8'))
        return cont

    def collectData(self, data):
        data1min = data['last1min']
        data5min = data['last5min']
        data15min = data['last15min']
        self.parseData(data1min, "1min")
        self.parseData(data5min, "5min")
        self.parseData(data15min, "15min")

    def parseData(self, data, tag):
        cprAirborneMsgs = data['cpr']['airborne']
        positions = data['cpr']['global_ok'] + data['cpr']['local_ok']
        tracksAll = data['tracks']['all']
        tracksSingle = data['tracks']['single_message']
        numMsgs = data['messages']
        localSignal = data['local']['signal']
        localPeakSigs = data['local']['peak_signal']
        localStrSigs = data['local']['strong_signals']
        modeSaccepted = data['local']['accepted']

        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat('T')
        json_data = [{
                "measurement": "adsbStats_"+tag,
                "tags":{
                    "host": "piflight",
                    "function": "adsbStats",
                    "datatag": tag
                    },
                "time": timestamp,
                "fields":
                    {
                        "airborneMsgs": cprAirborneMsgs,
                        "positions": positions,
                        "tracksAll": tracksAll,
                        "tracksSingle": tracksSingle,
                        "numMsgs": numMsgs,
                        "meanSignal": localSignal,
                        "peakSignal": localPeakSigs,
                        "strongSignals": localStrSigs,
                        "modeSaccepted": modeSaccepted[0]
                        }
                }]
        client = InfluxDBClient(host=self.host, port=8086, username='grafana', password='piGrafana', database='adsb')
        client.write_points(json_data, database='adsb', protocol='json')

if __name__ == '__main__':
    host = '192.168.0.47'
    d1090 = dump1090Reader(host)
    data = d1090.getData()
    d1090.collectData(data)
