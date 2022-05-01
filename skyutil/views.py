import os
import pyarrow as pa
import pyarrow.dataset as pa_ds


class View(object):
    def __init__(self, views_dir, dataset_path):
        self.views_dir = views_dir
        self.dataset_path = dataset_path

    def create_view(self, query, view_name, update=False):
        raise NotImplementedError("Implement in subclass")

    def get_view(self, view_name):
        raise NotImplementedError("Implement in subclass")

    def delete_view(self, view_name):
        if not os.path.exists(os.path.join(self.views_dir, view_name)):
            raise Exception("View {} does not exist".format(view_name))
        os.remove(os.path.join(self.views_dir, view_name))

    def list_views(self):
        if os.path.exists(self.views_dir):
            return os.listdir(self.views_dir)
    

class StaticView(View):
    def __init__(self, views_dir, dataset_path):
        super.__init__(self, views_dir, dataset_path)

    def create_view(self, query, view_name, update=False):
        if not os.path.exists(self.views_dir):
            os.makedirs(self.views_dir)

        if os.path.exists(os.path.join(self.views_dir, view_name)):
            if not update:
                raise Exception("View {} already exists.".format(view_name))

        table = pa_ds.dataset(self.dataset_path).to_table(filter=query)
        pa_ds.write_dataset(table, os.path.join(self.views_dir, view_name))
        return True

    def get_view(self, view_name):
        if not os.path.exists(os.path.join(self.views_dir, view_name)):
            raise Exception("View {} does not exist".format(view_name))

        with pa.ipc.RecordBatchFileReader(os.path.join(self.views_dir, view_name)) as reader:
            return reader.read_all()


class LazyView(View):
    def __init__(self, views_dir, dataset_path):
        super().__init__(self, views_dir, dataset_path)

    def create_view(self, query, view_name, update=False):
        if not os.path.exists(self.views_dir):
            os.makedirs(self.views_dir)

        if os.path.exists(os.path.join(self.views_dir, view_name)):
            if not update:
                raise Exception("View {} already exists.".format(view_name))
        
        with open(os.path.join(self.views_dir, view_name), "w") as f:
            f.write(query)
        return True

    def get_view(self, view_name):
        if not os.path.exists(os.path.join(self.views_dir, view_name)):
            raise Exception("View {} does not exist".format(view_name))
        
        with open(os.path.join(self.views_dir, view_name), "r") as f:
            query = f.read()

        return pa_ds.dataset(self.dataset_path).to_table(filter=query)
