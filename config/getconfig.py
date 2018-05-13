import os

def GetPara(path):
    paraDict = {}
    file = open(path)
    lines = file.readlines()
    for line in lines:
        try:
            line = line.replace('\r\n', '')
            line = line.replace('\n', '')
            strSplit = line.split(',')
            paraDict[strSplit[0]] = strSplit[1]
        except Exception as e:
            continue
    file.close()

    return paraDict