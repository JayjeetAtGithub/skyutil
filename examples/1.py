from skyutil.views import LazyView

if __name__ == "__main__":
    query = ('total_amount', '>', 100)
    view = LazyView("/tmp/views", "nyc.parquet")
    