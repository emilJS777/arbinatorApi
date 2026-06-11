from src import db

class Model:
    id = db.Column(db.Integer, primary_key=True)

    # SAVE DB SELF
    def save_db(self):
        db.session.add(self)
        db.session.commit()

    # SAVE LIST DB
    @staticmethod
    def save_all_db(model_list: list):
        db.session.add_all(model_list)
        db.session.commit()

    # DELETE DB
    def delete_db(self):
        db.session.delete(self)
        db.session.commit()

    # UPDATE DATABASE
    @staticmethod
    def update_db():
        db.session.commit()