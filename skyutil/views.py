import os
import pyarrow.dataset as pa_ds

VIEWS_DIR = "/mnt/cephfs/view"

def create_view(table, view_name, **kwargs):
    if not os.path.exists(VIEWS_DIR):
        os.makedirs(VIEWS_DIR)

    if os.path.exists(os.path.join(VIEWS_DIR, view_name)):
        raise Exception("View {} already exists.".format(view_name))

    return pa_ds.write_dataset(table, os.path.join(VIEWS_DIR, view_name), **kwargs)


def delete_view(view_name):
    if not os.path.exists(os.path.join(VIEWS_DIR, view_name)):
        raise Exception("View {} does not exist".format(view_name))
    os.remove(os.path.join(VIEWS_DIR, view_name))


def view(view_name, **kwargs):
    if not os.path.exists(os.path.join(VIEWS_DIR, view_name)):
        raise Exception("View {} does not exist".format(view_name))
    return pa_ds.dataset(os.path.join(VIEWS_DIR, view_name), format="parquet", **kwargs)
