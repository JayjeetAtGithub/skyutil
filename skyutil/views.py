import os
import pyarrow.dataset as pa_ds


class Views(object):
    def __init__(self, views_dir):
        self.views_dir = views_dir

    def create_view(self, table, view_name, update=False, **kwargs):
        if not os.path.exists(self.views_dir):
            os.makedirs(self.views_dir)

        if os.path.exists(os.path.join(self.views_dir, view_name)):
            if not update:
                raise Exception("View {} already exists.".format(view_name))

        return pa_ds.write_dataset(table, os.path.join(self.views_dir, view_name), **kwargs)

    def delete_view(self, view_name):
        if not os.path.exists(os.path.join(self.views_dir, view_name)):
            raise Exception("View {} does not exist".format(view_name))
        os.remove(os.path.join(self.views_dir, view_name))

    def get_view(self, view_name, **kwargs):
        if not os.path.exists(os.path.join(self.views_dir, view_name)):
            raise Exception("View {} does not exist".format(view_name))
        return pa_ds.dataset(os.path.join(self.views_dir, view_name), format="parquet", **kwargs)

    def list_views(self):
        if os.path.exists(self.views_dir):
            return os.listdir(self.views_dir)
