import snowflake.connector
import os
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

with open("/mnt/d/parmfile_prsing/rsa1_key.p8", "rb") as key:
    p_key= serialization.load_pem_private_key(
        key.read(),
        password=os.environ['snowflakey_passphrase'].encode(),
        backend=default_backend()
    )
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption())
    
ctx =snowflake.connector.connect(
    user='VINUTHA1987',
    account='xn86212.us-east-2.aws',
    private_key=pkb,
    warehouse=COMPUTE_WH,
    database=MOVIE,
    schema=PROD
    )


cs = ctx.cursor()
print(cs)
