from datetime import datetime

def queryAllMacroSignals(ticker, qqqMacro):
    masterList = []

    def processLine(txt):
        i0 = ticker
        txtList = txt.split(" ")
        txtList = txtList[1:]
        i1 = txtList[0] + " " + txtList[1][:-1]
        i2 = False if txtList[2][0] == "F" else True
        i3 = False if txtList[3][0] == "F" else True
        if txtList[5][0] == "n":
            return
        i4 = float(txtList[5][:-1])
        i5 = float(txtList[7][:-1])
        i6 = float(txtList[9][:-1])
        i7 = txtList[10][2:]
        i7 = False if i7[0] == "F" else True
        i8 = i1[:-2] + "00"
        final = [i0, i1, i2, i3, i4, i5, i6, i7, i8]
        masterList.append(final)

    
    qqqMacroLines = qqqMacro.readlines()

    for each in qqqMacroLines:
        processLine(each)

    base = """INSERT INTO "macro_signals" (ticker, timestamp, long, short, long_prob, flat_prob, short_prob, filter, timestamp_minutes) VALUES"""

    # createTable = """CREATE TABLE IF NOT EXISTS public.macro_signals
    # (
    # id integer NOT NULL DEFAULT nextval('macro_signals_id_seq'::regclass),
    # ticker character varying COLLATE pg_catalog."default",
    # "timestamp" timestamp without time zone,
    # "long" boolean,
    # short boolean,
    # long_prob numeric,
    # flat_prob numeric,
    # short_prob numeric,
    # filter boolean,
    # timestamp_minutes timestamp without time zone,
    # CONSTRAINT macro_signals_pkey PRIMARY KEY (id)
    # );"""
    # base = createTable + " " + base

    query = ""
    finalIdx = len(masterList)-1

    for i, e in enumerate(masterList):
        addThis = f""" ('{e[0]}', TIMESTAMP '{e[1]}', {e[2]}, {e[3]}, {e[4]}, {e[5]}, {e[6]}, {e[7]}, TIMESTAMP '{e[8]}')"""
        if i != 0:
            query = query + addThis
        else:
            query = base + addThis
        if i != finalIdx:
            query = query + ","

    query = query + ";"
    return query


