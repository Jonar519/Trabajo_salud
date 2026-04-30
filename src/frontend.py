import argparse

from frontend_server import run_frontend_server


def _parse_args(argv=None):
    p = argparse.ArgumentParser(add_help=True)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--csv", default=None)
    p.add_argument("--pbix", default=None)
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    run_frontend_server(host=args.host, port=args.port, csv_path=args.csv, pbix_path=args.pbix)
