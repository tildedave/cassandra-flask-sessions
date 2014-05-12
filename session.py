import pickle
import uuid

from cassandra.cluster import Cluster
from werkzeug.datastructures import CallbackDict
from flask.sessions import SessionInterface, SessionMixin

class CassandraSession(CallbackDict, SessionMixin):
    def __init__(self, initial=None, sid=None, new=False):
        def on_update(self):
            self.modified = True

        CallbackDict.__init__(self, initial, on_update)
        self.sid = sid
        self.new = new
        self.modified = False


class CassandraSessionInterface(SessionInterface):
    serializer = pickle
    session_class = CassandraSession

    select_query = "SELECT * FROM sessions WHERE session_key = %s"
    delete_query = "DELETE FROM sessions WHERE session_key = %s"
    insert_query = "INSERT INTO sessions (session_key, session_data) VALUES (%s, %s) USING TTL %s"

    create_session_query = """
CREATE COLUMNFAMILY IF NOT EXISTS sessions (
    session_key             text,
    session_data            text,
    PRIMARY KEY             (session_key)
)
"""

    def __init__(self, session=None, prefix='session:'):
        if session is None:
            cluster = Cluster()

            self.session = cluster.connect('demo')
            self.session.execute(self.create_session_query)

            self.prefix = prefix

    def generate_sid(self):
        return str(uuid.uuid4())

    def open_session(self, app, request):
        sid = request.cookies.get(app.session_cookie_name)
        if not sid:
            sid = self.generate_sid()
            return self.session_class(sid=sid, new=True)

        rows = self.session.execute(self.select_query,
                                    (self.prefix + sid,))

        if rows:
            data = self.serializer.loads(rows[0].session_data)
            return self.session_class(data, sid=sid)

        return self.session_class(sid=sid, new=True)

    def save_session(self, app, session, response):
        domain = self.get_cookie_domain(app)
        if not session:
            self.session.execute(self.delete_query,
                                 (self.prefix + session.sid,))
            if session.modified:
                response.delete_cookie(app.session_cookie_name,
                                       domain=domain)
            return

        cookie_exp = self.get_expiration_time(app, session)
        val = self.serializer.dumps(dict(session))
        exp_time = int(60 * 60)  # 60 minutes * 60 seconds per minute = 3600 seconds

        self.session.execute(self.insert_query,
                             (self.prefix + session.sid, val, exp_time))

        response.set_cookie(app.session_cookie_name, session.sid,
                            expires=cookie_exp, httponly=True,
                            domain=domain)
