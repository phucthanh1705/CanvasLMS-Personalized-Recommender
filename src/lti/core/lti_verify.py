# src/lti/core/lti_verify.py

import jwt
from jwt import PyJWKClient

def verify_id_token(
    *,
    id_token: str,
    canvas_jwks_url: str,
    client_id: str
) -> dict:
    """
    Verify id_token từ Canvas và trả về payload đã decode
    """

    if not id_token:
        raise ValueError("Missing id_token")

    jwk_client = PyJWKClient(canvas_jwks_url)
    signing_key = jwk_client.get_signing_key_from_jwt(id_token).key

    decoded = jwt.decode(
        id_token,
        signing_key,
        algorithms=["RS256"],
        options={
            "verify_aud": False
        }
    )

    # TỰ CHECK audience
    aud = decoded.get("aud", [])
    if isinstance(aud, str):
        aud = [aud]

    if client_id not in aud:
        raise Exception(f"Audience mismatch: {aud} != {client_id}")

    return decoded
