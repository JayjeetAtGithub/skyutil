from skyutil.views import LazyView, StaticView

if __name__ == "__main__":
    query = ('total_amount', '>', 100)
    view = LazyView("/tmp/views", "nyc.parquet")
    view.create(query, "view_total_amount_gt_100", update=True)
    print(view.get("view_total_amount_gt_100"))

    query = ('total_amount', '>', 50)
    view = StaticView("/tmp/views", "nyc.parquet")
    view.create(query, "view_total_amount_gt_50", update=True)
    print(view.get("view_total_amount_gt_50"))