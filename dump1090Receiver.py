#!/usr/bin/python3

import urllib.request
import json
from influxdb import InfluxDBClient
import datetime
import argparse

class dump1090Reader:
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.url = 'http://'+host+'/dump1090/data/'

    def getAircraft(self):
        req = urllib.request.Request(self.url+'aircraft.json')
        r = urllib.request.urlopen(req).read()
        cont = json.loads(r.decode('utf-8'))
        return cont

    def getStats(self):
        req = urllib.request.Request(self.url+'stats.json')
        r = urllib.request.urlopen(req).read()
        cont = json.loads(r.decode('utf-8'))
        return cont

    def collectStats(self, data):
        data1min = data['last1min']
        data5min = data['last5min']
        data15min = data['last15min']
        self.parseStats(data1min, "1min")
        self.parseStats(data5min, "5min")
        self.parseStats(data15min, "15min")


    def parseStats(self, data, tag):
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
        client = InfluxDBClient(host=self.host, port=8086, username=self.user, password=self.password, database='adsb')
        client.write_points(json_data, database='adsb', protocol='json')

    def parseAircraft(self, data):
        numAircrafts = len(data['aircraft'])
        timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat('T')
        json_data = [{
                "measurement": "adsbAircrafts",
                "tags":{
                    "host": "piflight",
                    "function": "adsbAircraft",
                    "datatag": "aircrafts"
                    },
                "time": timestamp,
                "fields":
                    {
                        "numAircrafts": numAircrafts
                        }
                    }]
        client = InfluxDBClient(host=self.host, port=8086, username='grafana', password='piGrafana', database='adsb')
        client.write_points(json_data, database='adsb', protocol='json')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='dump1090 logger')
    parser.add_argument('host', type=str, help='ip-address of influxdb host')
    parser.add_argument('user', type=str, help='username for influxdb')
    parser.add_argument('password', type=str, help='password for influxdb')

    args = parser.parse_args()

    d1090 = dump1090Reader(args.host, args.user, args.password)
    stats = d1090.getStats()
    d1090.collectStats(stats)
    aircrafts = d1090.getAircraft()
    d1090.parseAircraft(aircrafts)
