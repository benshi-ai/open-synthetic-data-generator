from sqlalchemy.orm import Session


class DBSessionWrapper:
    def __init__(self, db_session: Session):
        self._db_session = db_session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()

    @property
    def query(self):
        return self._db_session.query

    @property
    def add(self):
        return self._db_session.add

    @property
    def close(self):
        return self._db_session.close

    def commit(self):
        try:
            self._db_session.commit()
        except Exception:
            self._db_session.rollback()
            raise
