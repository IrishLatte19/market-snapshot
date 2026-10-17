# Start the pipeline from the command line with python fetch.py.
# This file passes control to app.py, which handles the selected command.

from pipeline.app import main

if __name__ == '__main__':
    raise SystemExit(main())
