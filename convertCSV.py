
class LogServer():
    file_name = 'airquality-glensserver.com-access_log1'
    f = open(file_name)

    line = f.readline()
    csv_file = open(file_name + ".csv", "w")
    while line:
        csv_data = line.replace("[", "").replace("]", ",").replace(" +", ",+").replace(" \"", "").replace(r" /",
                                                                                                          ",").replace(
            "\" ", ",").replace(" ", ",").replace(",H", " H")
        csv_file.write(csv_data)
        line = f.readline()
    csv_file.close()
    f.close()
