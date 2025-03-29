import pandas as pd
import numpy as np

kh970_cb1_cmds = {
    '0x07' : 'confirm receipt', 
    '0x87' : 'end of message',
    '0x8B' : 'nothing to say?'
}

kh970_bed_cmds = {
    '0x0D' : 'eol/direction change?', 
    '0x4D' : 'eol/direction change', 
    '0x6D' : 'eol/direction change?', 
    '0x85' : 'row counter hit', 
    '0x89' : 'pattern fully received',
    '0x8D' : 'eol/direction change', 
}

def byte2word(df, n):
    df = (
        df.groupby(df.index // 2)
        .agg(''.join)
        .map(lambda x: x.replace('0x', ''))
        .map(lambda x: '0x' + x)
        .to_frame()
    )
    return df

def invert_hex(df, col):
    invert = (
        df[col].map(lambda x: hex(np.invert(np.uint8(int(x,0)))))
        .map(lambda x: x if len(x) == 4 else x.replace('0x', '0x0'))
        .map(lambda x: x.upper().replace('X', 'x'))
        .to_list()
    )
    return invert

def decode_cmd(df, index):

    if(index == len(df.index) - 1):
        return 'END'

    decode = ""
    mosi_idx = df.columns.get_loc('mosi')
    miso_idx = df.columns.get_loc('~miso')

    bed_prev = df.iat[index - 1, miso_idx]
    bed_now = df.iat[index, miso_idx]
    bed_next = df.iat[index + 1, miso_idx]

    cb1_prev = df.iat[index - 1, mosi_idx]
    cb1_now = df.iat[index, mosi_idx]
    cb1_next = df.iat[index + 1, mosi_idx]
    
    try: 
        if cb1_now == '0x47' and bed_now == '0x01':
            decode += 'CB1 (HEADER)'
        elif cb1_now == '0x87':
            decode += 'EOM'
        elif cb1_now == bed_prev and bed_now == cb1_prev:
            decode += 'ACK'
        elif bed_now == cb1_prev:
            decode += 'PATTERN DATA'
        elif bed_now == cb1_next:
            decode += 'BED'
        elif cb1_now == bed_next:
            decode += 'CB1'
        else:
            return ""

        return decode

    except:
        print('There is a problem with the decode.')

def fix_pattern_data(df, index):
    decode_idx = df.columns.get_loc('decoded')
    mosi_idx = df.columns.get_loc('mosi')
    miso_idx = df.columns.get_loc('~miso')

    if index == len(df.index) - 1:
        return df.iat[index, decode_idx]

    decode_prev = df.iat[index - 1, decode_idx]
    decode_now = df.iat[index, decode_idx]

    cb1_prev = df.iat[index - 1, mosi_idx]
    cb1_now = df.iat[index, mosi_idx]

    if decode_now == 'CB1' and cb1_prev == '0x85':
        df.iat[index, decode_idx] = 'PATTERN DATA'

    if decode_now == 'ACK' and decode_prev == 'PATTERN DATA' and cb1_now != '0x87':
        df.iat[index, decode_idx] = 'PATTERN DATA'

    return df.iat[index, decode_idx]

def cmd_semantics(df, index):
    decode_idx = df.columns.get_loc('decoded')
    mosi_idx = df.columns.get_loc('mosi')
    miso_idx = df.columns.get_loc('~miso')

    cmd = df.iat[index, decode_idx]
    try:
        if cmd == 'CB1':
            return kh970_cb1_cmds[df.iat[index, mosi_idx]]
        elif cmd == 'BED': 
            return kh970_bed_cmds[df.iat[index, miso_idx]]
        else:
            return ''

    except KeyError:
        return '???'

    except:
        return ''

def sort_cmd(df, cmd, index):
    decode_idx = df.columns.get_loc('decoded')
    mosi_idx = df.columns.get_loc('mosi')
    miso_idx = df.columns.get_loc('~miso')

    if df.iat[index, decode_idx] == cmd:
        if cmd == 'CB1':
            return df.iat[index, mosi_idx]
        elif cmd == 'BED':
            return df.iat[index, miso_idx]
    else:
        return ''

raw_data = pd.read_csv('kh970_p108_l.csv')
data = raw_data.query('type == "result"').sort_values('start_time').reset_index()

data['~miso'] = invert_hex(data, 'miso')
data['decoded'] = [decode_cmd(data, i) for i in data.index]
data['decoded'] = [fix_pattern_data(data, i) for i in data.index]
data['meaning'] = [cmd_semantics(data, i) for i in data.index]
data['CB1'] = [sort_cmd(data, "CB1", i) for i in data.index]
data['CB1_bin'] = data['CB1'].map(lambda x: bin(int(x,0))[2:].zfill(8) if x != '' else '').to_frame()
data['BED'] = [sort_cmd(data, "BED", i) for i in data.index]
data['BED_bin'] = data['BED'].map(lambda x: bin(int(x,0))[2:].zfill(8) if x != '' else '').to_frame()

data.to_csv('./results.csv')

unique_cb1 = data['CB1'].unique()
unique_bed = data['BED'].unique()

print('Unique CB1 commands: ', sorted(unique_cb1))
print('Unique BED commands: ', sorted(unique_bed))
