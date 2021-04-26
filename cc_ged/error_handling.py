import re
from functools import wraps
from fastapi import Response
from requests import HTTPError

def proxy_http_err(fn):
    @wraps(fn)
    def inner(*args,**kwargs):
        try:
            return fn(*args,**kwargs)
        except HTTPError as httpe:
            response = httpe.response
            if response.status_code == 400:
                try:
                    data = response.json()
                    data = {k.lower():v for k,v in data.items()}
                    msg = data["message"]
                    status = 404 if re.search("[Nn]o version",msg) else data.status_code
                    return Response(
                            msg,
                            status_code = status
                        )
                except (ValueError,KeyError):
                    pass
            return Response(
                    f"Proxied {response.content}",
                    status_code=response.status_code
                )
    return inner
