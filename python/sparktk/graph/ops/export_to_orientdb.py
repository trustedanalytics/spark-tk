
def export_to_orientdb(self, batch_size, db_url, user_name, password, root_password):
    self._scala.exportToOrientdb(batch_size, db_url, user_name, password, root_password)