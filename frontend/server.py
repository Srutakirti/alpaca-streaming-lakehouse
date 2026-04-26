import sys
import os

# Make `frontend.*` importable from the workspace root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("frontend.app.main:app", host="0.0.0.0", port=port)
