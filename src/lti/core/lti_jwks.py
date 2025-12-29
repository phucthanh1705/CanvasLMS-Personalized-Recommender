from flask import Blueprint, jsonify
import jwt
from cryptography.hazmat.primitives.asymmetric import rsa

jwks_bp = Blueprint("jwks_bp", __name__)

private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,
)

public_key = private_key.public_key()
public_numbers = public_key.public_numbers()

jwk = {
    "kty": "RSA",
    "use": "sig",
    "alg": "RS256",
    "kid": "mysmart-key",
    "n": jwt.utils.base64url_encode(
        public_numbers.n.to_bytes((public_numbers.n.bit_length() + 7) // 8, "big")
    ).decode(),
    "e": jwt.utils.base64url_encode(
        public_numbers.e.to_bytes((public_numbers.e.bit_length() + 7) // 8, "big")
    ).decode(),
}

jwks = {"keys": [jwk]}

@jwks_bp.route("/.well-known/jwks.json")
def serve_jwks():
    return jsonify(jwks)
