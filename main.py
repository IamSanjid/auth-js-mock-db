from flask import Flask, request, jsonify, g

# TODO: Replace with proper database
class CustomDB:
    _dbs = {}
    def __init__(self):
        self._dbs = {}

    def create_db(self, db_name):
        if db_name in self._dbs:
            raise ValueError(f"Database '{db_name}' already exists.")
        self._dbs[db_name] = {}

    def create_collection(self, db_name, collection_name):
        if db_name not in self._dbs:
            raise ValueError(f"Database '{db_name}' does not exist.")
        if collection_name in self._dbs[db_name]:
            return
        self._dbs[db_name][collection_name] = []

    def insert(self, db_name, collection_name, data):
        if db_name not in self._dbs:
            raise ValueError(f"Database '{db_name}' does not exist.")
        if collection_name not in self._dbs[db_name]:
            raise ValueError(f"Collection '{collection_name}' does not exist in database '{db_name}'.")
        self._dbs[db_name][collection_name].append(data)

    def find(self, db_name, collection_name, query: dict):
        if db_name not in self._dbs:
            raise ValueError(f"Database '{db_name}' does not exist.")
        if collection_name not in self._dbs[db_name]:
            raise ValueError(f"Collection '{collection_name}' does not exist in database '{db_name}'.")
        return [item for item in self._dbs[db_name][collection_name] if all(item.get(k) == v for k, v in query.items())]

    def delete(self, db_name, collection_name, query: dict, delete_many=True):
        if db_name not in self._dbs:
            raise ValueError(f"Database '{db_name}' does not exist.")
        if collection_name not in self._dbs[db_name]:
            raise ValueError(f"Collection '{collection_name}' does not exist in database '{db_name}'.")
        if delete_many:
            self._dbs[db_name][collection_name] = [item for item in self._dbs[db_name][collection_name] if not all(item.get(k) == v for k, v in query.items())]
        else:
            for item in self._dbs[db_name][collection_name]:
                if all(item.get(k) == v for k, v in query.items()):
                    self._dbs[db_name][collection_name].remove(item)
                    break

    def update(self, db_name, collection_name, query: dict, update_data: dict, find_one_and_update=False):
        if db_name not in self._dbs:
            raise ValueError(f"Database '{db_name}' does not exist.")
        if collection_name not in self._dbs[db_name]:
            raise ValueError(f"Collection '{collection_name}' does not exist in database '{db_name}'.")
        for item in self._dbs[db_name][collection_name]:
            if all(item.get(k) == v for k, v in query.items()):
                item.update(update_data)
                if find_one_and_update:
                    return item
        return None


DB_NAME = "test_db"
_db = None
def get_db():
    global _db
    if _db is None:
        _db = CustomDB()
        _db.create_db(DB_NAME)
    return _db

def create_app():
    app = Flask(__name__)

    # TODO: do stuff with `app.config`
    # TODO: add db stuff...

    @app.before_request
    def before_request():
        g.db = get_db()

    @app.route("/")
    def hello():
        return "Hello, Flask!"

    @app.route("/" + DB_NAME + "/collection/<collection_name>", methods=["POST"])
    def create_collection(collection_name):
        request_data = request.get_json()

        app.logger.debug(f"Creating collection: {collection_name}, {request_data}")

        # TODO: Check schema and table shits, it should be a list of dicts
        if not isinstance(request_data, list) or not all(isinstance(item, dict) for item in request_data):
            return jsonify({"error": "Invalid data format. Expected a list of dictionaries."}), 400

        g.db.create_collection(DB_NAME, collection_name)
        return jsonify({"message": f"Collection '{collection_name}' created!"}), 201

    # @app.route("/" + DB_NAME + "/<collection_name>/<string:id>", methods=["GET"])
    # def get_item_by_id(collection_name, id):
    #     app.logger.debug(f"Getting item by ID from collection: {collection_name}, {id}")

    #     query = {"id": id}
    #     result = g.db.find(DB_NAME, collection_name, query)
    #     if not result:
    #         return jsonify({"error": "Item not found."}), 404
    #     return jsonify(result[0]), 200
    
    @app.route("/" + DB_NAME + "/<collection_name>", methods=["GET"])
    def get_item(collection_name):
        request_data = request.get_json()

        app.logger.debug(f"Getting item from collection: {collection_name}, {request_data}")

        if not isinstance(request_data, dict):
            return jsonify({"error": "Invalid data format. Expected a dictionary."}), 400
        
        find_one = request_data.get("findOne", False)
        query = request_data.get("query", {})

        if find_one:
            # TODO: Optimize for single item retrieval
            result = g.db.find(DB_NAME, collection_name, query)
            if not result:
                return jsonify({"error": "Item not found."}), 404
            return jsonify(result[0]), 200

        result = g.db.find(DB_NAME, collection_name, query)
        return jsonify(result), 200
    
    @app.route("/" + DB_NAME + "/<collection_name>", methods=["POST"])
    def insert_item(collection_name):
        request_data = request.get_json()

        app.logger.debug(f"Inserting item into collection: {collection_name}, {request_data}")

        if not isinstance(request_data, dict):
            return jsonify({"error": "Invalid data format. Expected a dictionary."}), 400
        
        insert_one = request_data.get("insertOne", False)
        query = request_data.get("query", {})

        if len(query) == 0:
            return jsonify({"error": "Invalid data. No fields provided."}), 400

        # TODO: It will matter when actual DB is used        
        if insert_one:
            app.logger.info("Inserting one item")

        g.db.insert(DB_NAME, collection_name, query)
        return jsonify({"message": "Item inserted!"}), 201
    
    @app.route("/" + DB_NAME + "/<collection_name>", methods=["DELETE"])
    def delete_item(collection_name):
        request_data = request.get_json()

        app.logger.debug(f"Deleting item from collection: {collection_name}, {request_data}")

        if not isinstance(request_data, dict):
            return jsonify({"error": "Invalid data format. Expected a dictionary."}), 400
        
        delete_many = request_data.get("deleteMany", False)
        find_one_and_delete = request_data.get("findOne", False)
        query = request_data.get("query", {})

        if len(query) == 0:
            return jsonify({"error": "Invalid data. No fields provided."}), 400

        result = None
        if find_one_and_delete:
            delete_many = False
            result = g.db.find(DB_NAME, collection_name, query)
            if not result:
                return jsonify({"error": "Item not found."}), 404

        g.db.delete(DB_NAME, collection_name, query, delete_many=delete_many)
        if find_one_and_delete:
            return jsonify(result[0]), 200
        return jsonify({"message": "Item deleted!"}), 200
    
    @app.route("/" + DB_NAME + "/<collection_name>", methods=["PUT"])
    def update_item(collection_name):
        request_data = request.get_json()

        app.logger.debug(f"Updating item in collection: {collection_name}, {request_data}")

        if not isinstance(request_data, dict):
            return jsonify({"error": "Invalid data format. Expected a dictionary."}), 400

        query = request_data.get("query", {})
        update = request_data.get("update", {})
        find_one_and_update = request_data.get("findOneAndUpdate", False)
        found = g.db.update(DB_NAME, collection_name, query, update, find_one_and_update=find_one_and_update)
        if not found:
            return jsonify(g.db.find(DB_NAME, collection_name, query)[0]), 200
        return jsonify(found), 200
    
    @app.route("/" + DB_NAME + "/<collection_name>/<string:id>", methods=["PUT"])
    def update_item_by_id(collection_name, id):
        request_data = request.get_json()

        app.logger.debug(f"Updating item by ID in collection: {collection_name}, {id}, {request_data}")

        if not isinstance(request_data, dict):
            return jsonify({"error": "Invalid data format. Expected a dictionary."}), 400

        query = {"id": id}
        g.db.update(DB_NAME, collection_name, query, request_data)
        return jsonify(g.db.find(DB_NAME, collection_name, query)[0]), 200

    return app