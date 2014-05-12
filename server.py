from flask import Flask, session
from session import CassandraSessionInterface

app = Flask(__name__)
app.session_interface = CassandraSessionInterface()

@app.route('/')
def hello_world():
    if 'times' not in session:
        session['times'] = 0
    session['times'] += 1

    return("You have visted the page {0} time(s)".format(session['times']))


if __name__ == "__main__":
    app.run()
