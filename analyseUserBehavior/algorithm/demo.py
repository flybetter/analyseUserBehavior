with open("select_CONTEXT_ID_from_DWB_DA_APP_STATIS.csv") as f:
    for line in f.readlines():
        if "-" not in line:
            print(line)
