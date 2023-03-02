import base64
import binascii
import contextlib
import paramiko
from io import StringIO
from typing import Tuple


class SomeSSHException(Exception):
    pass


def get_private_key(input_key, private_key_password):
    key = _get_decoded_key(input_key)
    try:
        if private_key_password:
            return paramiko.RSAKey.from_private_key(StringIO(key), password=private_key_password)
        else:
            return paramiko.RSAKey.from_private_key(StringIO(key))
    except paramiko.ssh_exception.SSHException as pkey_error:
        raise SomeSSHException("Invalid private key")from pkey_error


def _get_decoded_key(input_key):
    """
        Have to satisfy both encoded and not encoded keys
    """
    b64_decoded_input_key = ""
    with contextlib.suppress(binascii.Error):
        b64_decoded_input_key = base64.b64decode(input_key, validate=True).decode('utf-8')

    is_valid_b64, message_b64 = validate_ssh_private_key(b64_decoded_input_key)
    is_valid, message = validate_ssh_private_key(input_key)
    if is_valid_b64:
        final_key = b64_decoded_input_key
    elif is_valid:
        final_key = input_key
    else:
        raise SomeSSHException("\n".join([message, message_b64]))
    return final_key


def validate_ssh_private_key(ssh_private_key: str) -> Tuple[bool, str]:
    if "\n" not in ssh_private_key:
        return False, "SSH Private key is invalid, make sure it \\n characters as new lines"
    return True, ""
