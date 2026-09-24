FROM python:3.12-slim

# psycopg 3.2 is the floor: conn.notifies(timeout=...) only exists from there,
# and without it the listen loop cannot wake up for the daily trigger check.
RUN pip install --no-cache-dir "psycopg[binary]>=3.2,<4"

WORKDIR /app
COPY watcher.py /app/watcher.py
COPY sql /app/sql

ENV STATE_FILE=/state/last_seen
VOLUME ["/state"]

# No published port, so no HTTP healthcheck is possible. The loop touches a
# heartbeat file every time it wakes (at most 30s apart), so staleness catches a
# hung LISTEN as well as a dead process - which checking our own PID would not.
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s \
  CMD python -c "import os,sys,time; sys.exit(0 if time.time()-os.path.getmtime(os.environ.get('HEARTBEAT_FILE','/state/heartbeat')) < 180 else 1)"

USER 1000:100
CMD ["python", "-u", "/app/watcher.py"]
