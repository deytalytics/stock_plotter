from flask import Flask
from authlib.integrations.flask_client import OAuth
from flask_login import LoginManager, login_user, current_user, login_required
import os

oauth = OAuth()
login_manager = LoginManager()

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

# User loader callback
@login_manager.user_loader
def load_user(user_id):
    # Here, you should write the logic to load a user from your database using the user_id
    # and return the user object. For now, let's assume that the user object is stored in session.
    return session.get('user')

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080)
