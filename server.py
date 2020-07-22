import socket
import threading
import time
from ConnectionPool import ConnectionPool


def sendfaciID(sock, pool, serialID):
    id = pool.execute("select facilityid from facility where serialflag=\"" + serialID + "\";", return_one=True)[0]
    for i in range(10):
        sock.send(bytes(";faciID," + str(id) + ";", encoding='utf-8'))
        time.sleep(10)


def solveData(data, pool, sock):
    dataStr = str(data, encoding='utf-8').split(';')[0]
    dataList = dataStr.split(',')
    sql = ""
    if dataList[0] == 'hr':
        sql = "insert into hr(facilityid, inserttime, hr) values (" + dataList[1] + ', now(), ' + dataList[2] + ');'
        pass
    if dataList[0] == 'facility':
        sql = "insert into facility(serialflag, intnetflag) values (\"" + dataList[1] + '\", now());'
    if dataList[0] == 'patient':
        sql = "insert into patient(idcardnum,`name`, age, sex) values (" + '\"'+dataList[1]+'\",' + '\"'+dataList[2]+'\",' + dataList[3] + ',\"'+dataList[4]+'\"' + ');'
    if dataList[0] == 'st':
        sql = "insert into st(facilityid, inserttime, st1, st2, pvc) values (" + dataList[1] + ', now(), ' + dataList[2] + ',' + dataList[3] + ',' + dataList[4] + ');'
    if dataList[0] == 'nibp':
        sql = "insert into nibp(facilityid, inserttime, highpress, lowpress, averagepress) values (" + dataList[1] + ', now(), ' + dataList[2] + ',' + dataList[3] + ',' + dataList[4] + ');'
    if dataList[0] == 'temp':
        sql = "insert into temp(facilityid, inserttime, t1, t2, td) values (" + dataList[1] + ', now(), ' + dataList[2] + ',' + dataList[3] + ',' + dataList[4] + ');'
    if dataList[0] == 'resp':
        sql = "insert into resp(facilityid, inserttime, resp) values (" + dataList[1] + ', now(), ' + dataList[2] + ');'
    if dataList[0] == 'wave':
        sql = "insert into wave(facilityid, inserttime, ecg, spo2, resp) values (" + dataList[1] + ', now(), ' + \
              dataList[2] + ',' + dataList[3] + ',' + dataList[4] + ');'
    if dataList[0] == 'co2':
        sql = "insert into co2(facilityid, inserttime, co2, awrr, ins) values (" + dataList[1] + ', now(), ' + dataList[2] + ',' + dataList[3] + ',' + dataList[4] + ');'
    if dataList[0] == 'spo2':
        sql = "insert into spo2(facilityid, inserttime, spo2, pulse) values (" + dataList[1] + ', now(), ' + dataList[2] + ',' + dataList[3] + ');'
    if dataList[0] == 'ecgplot':
        sql = "insert into ecgwave(facilityid, inserttime, ecgdata) values (" + dataList[1] + ', now(), \"' + dataList[2] + '\");'
    if dataList[0] == 'spo2plot':
        sql = "insert into spo2wave(facilityid, inserttime, spo2data) values (" + dataList[1] + ', now(), \"' + dataList[2] + '\");'
    if dataList[0] == 'bpplot':
        sql = "insert into bpwave(facilityid, inserttime, bpdata) values (" + dataList[1] + ', now(), \"' + dataList[2] + '\");'
    if sql != "":
        try:
            pool.execute(sql)
        except:
            pass
    if dataList[0] == 'facility':
        t = threading.Thread(target=sendfaciID, args=(sock, pool, dataList[1]))
        t.start()


def solve(sock, addr, pool):
    while True:
        data = sock.recv(2300)
        if not data:
            break
        solveData(data, pool, sock)
        time.sleep(1)
    sock.close()
    return 0


def main():
    pool = ConnectionPool(host="127.0.0.1", port=3306, user="root", passwd="465341123", db="justfortest")
    #db = pymysql.connect("127.0.0.1", "root", "465341123", "justfortest")
    #cursor = db.cursor()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ipStr, portStr = ("0.0.0.0", "2000")
    s.bind((ipStr, int(portStr)))
    s.listen(int(portStr))
    while True:
        sock, addr = s.accept()
        t = threading.Thread(target=solve, args=(sock, addr, pool))
        t.start()


if __name__ == "__main__":
    main()
