import algosdk

address = 'E5SDQTVXEKCXRUW35MFUOC6PBT62COIGPOW44ISCLR2YV3WA6ZQURZR5DI'

private_phrase = """
medal
fire
baby
emerge
pledge
visit
address
caution
addict
cluster
pair
sentence
enforce
student
memory
off
please
flower
spray
program
net
april
critic
ability
case"""

def get_private_key():
    return algosdk.mnemonic.to_private_key(private_phrase)


