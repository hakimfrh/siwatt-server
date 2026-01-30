from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi import Request
from pydantic import BaseModel
from app.routers import auth, token, dashboard, data_hourly, profile
from app.routers import device

app = FastAPI(title="SIWATT API")

app.include_router(auth.router)
app.include_router(profile.router)
app.include_router(device.router)
app.include_router(token.router)
app.include_router(dashboard.router)
app.include_router(data_hourly.router)

@app.get("/")
def root():
    return {"status": "SIWATT backend running"}


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = {}

    # ambil schema dari endpoint
    route = request.scope.get("route")
    body_model = None

    if route:
        for dep in route.dependant.body_params:
            if issubclass(dep.type_, BaseModel):
                body_model = dep.type_

    for err in exc.errors():
        loc = err["loc"]

        # kalau body kosong
        if loc == ("body",) and body_model:
            for field in body_model.model_fields.keys():
                errors[field] = "Field required"
        else:
            field = loc[-1]
            errors[field] = err["msg"]

    return JSONResponse(
        status_code=422,
        content={
            "code": 422,
            "message": "Validation error",
            "errors": errors
        }
    )
