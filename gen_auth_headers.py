import base64
import datetime
import os

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from dotenv import load_dotenv

load_dotenv()


def sign_pss_text(private_key: rsa.RSAPrivateKey, text: str) -> str:
    message = text.encode('utf-8')
    try:
        signature = private_key.sign(message, padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                                                          salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
        return base64.b64encode(signature).decode('utf-8')
    except InvalidSignature as e:
        raise ValueError("RSA sign PSS failed") from e


def gen_kalshi_auth_headers(method: str, path: str) -> dict:
    current_time = datetime.datetime.now()
    timestamp = current_time.timestamp()
    current_time_milliseconds = int(timestamp * 1000)
    timestampt_str = str(current_time_milliseconds)

    private_key = serialization.load_pem_private_key(base64.b64decode(os.getenv('KALSHI_PRIVATE_KEY')), password=None)

    path_without_query = path.split('?')[0]
    msg_string = timestampt_str + method + path_without_query
    sig = sign_pss_text(private_key, msg_string)

    return {'KALSHI-ACCESS-KEY': os.getenv('KALSHI_PUBLIC_KEY'), 'KALSHI-ACCESS-SIGNATURE': sig,
        'KALSHI-ACCESS-TIMESTAMP': timestampt_str}
