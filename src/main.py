from flask import Flask

from lti.core.lti_login import lti_login_bp
from lti.core.lti_jwks import jwks_bp
from lti.chat.launch import chat_bp
from lti.chat.routes import chat_routes_bp
from lti.recommender.launch import recommender_bp

def create_app():
    app = Flask(__name__)

    app.register_blueprint(lti_login_bp) 
    app.register_blueprint(jwks_bp)         
    app.register_blueprint(recommender_bp, url_prefix="")
    app.register_blueprint(chat_bp)
    app.register_blueprint(chat_routes_bp)

    @app.route("/")
    def index():
        return "SmartSchool LTI is running"

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(
        host="0.0.0.0",
        port=5000,
        debug=True
    )
