from flask import Flask
from authlib.integrations.flask_client import OAuth
import os

oauth = OAuth()

def create_app():

    app = Flask(__name__)
    app.config.from_object('config')
    CONF_URL = 'https://accounts.google.com/.well-known/openid-configuration'

    # Initialize oauth with the app
    oauth.init_app(app)

    oauth.register(
        name='google',
        server_metadata_url=CONF_URL,
        client_kwargs={
            'scope': 'openid email profile'
        }
    )

    from views import routes
    app.register_blueprint(routes)

    return app

app = create_app()
app.secret_key=os.getenv('SECRET_KEY')

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080)
