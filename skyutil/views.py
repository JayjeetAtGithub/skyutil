import os
import pyarrow as pa
import pyarrow.dataset as pa_ds


class View(object):
    def __init__(self, views_dir, dataset_path):
        self.views_dir = views_dir
        self.dataset_path = dataset_path

    def create(self, query, view_name, update=False):
        raise NotImplementedError("Implement in subclass")

    def get(self, view_name):
        raise NotImplementedError("Implement in subclass")

    def delete(self, view_name):
        if not os.path.exists(os.path.join(self.views_dir, view_name)):
            raise Exception("View {} does not exist".format(view_name))
        os.remove(os.path.join(self.views_dir, view_name))

    def list(self):
        if os.path.exists(self.views_dir):
            return os.listdir(self.views_dir)

    def _convert_pd_expr_to_arrow(self, expr):
        arrow_expr = ()
        field, comparator, value = expr
        field = pa_ds.field(field) 
        if comparator == "=":
            arrow_expr += (field == value)
        elif comparator == "!=":
            arrow_expr += (field != value)
        elif comparator == ">":
            arrow_expr += (field > value)
        elif comparator == ">=":
            arrow_expr += (field >= value)
        elif comparator == "<":
            arrow_expr += (field < value)
        elif comparator == "<=":
            arrow_expr += (field <= value)
        else:
            raise Exception("Unknown comparator {}".format(comparator))
        return arrow_expr


class StaticView(View):
    def __init__(self, views_dir, dataset_path):
        super.__init__(self, views_dir, dataset_path)

    def create(self, query, view_name, update=False):
        if not os.path.exists(self.views_dir):
            os.makedirs(self.views_dir)

        if os.path.exists(os.path.join(self.views_dir, view_name)):
            if not update:
                raise Exception("View {} already exists.".format(view_name))

        table = pa_ds.dataset(self.dataset_path).to_table(filter=self._convert_pd_expr_to_arrow(query))
        pa_ds.write_dataset(table, os.path.join(self.views_dir, view_name))
        return True

    def get(self, view_name):
        if not os.path.exists(os.path.join(self.views_dir, view_name)):
            raise Exception("View {} does not exist".format(view_name))

        with pa.ipc.RecordBatchFileReader(os.path.join(self.views_dir, view_name)) as reader:
            return reader.read_all()


class LazyView(View):
    def __init__(self, views_dir, dataset_path):
        super().__init__(self, views_dir, dataset_path)

    def create(self, query, view_name, update=False):
        if not os.path.exists(self.views_dir):
            os.makedirs(self.views_dir)

        if os.path.exists(os.path.join(self.views_dir, view_name)):
            if not update:
                raise Exception("View {} already exists.".format(view_name))
        
        with open(os.path.join(self.views_dir, view_name), "w") as f:
            f.write(query)
        return True

    def get(self, view_name):
        if not os.path.exists(os.path.join(self.views_dir, view_name)):
            raise Exception("View {} does not exist".format(view_name))
        
        with open(os.path.join(self.views_dir, view_name), "r") as f:
            query = f.read()

        return pa_ds.dataset(self.dataset_path).to_table(filter=self._convert_pd_expr_to_arrow(query))
