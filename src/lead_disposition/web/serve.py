"""CLI entrypoint: disposition-serve"""

import uvicorn


def main() -> None:
    uvicorn.run(
        "lead_disposition.web.app:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
    )


if __name__ == "__main__":
    main()
