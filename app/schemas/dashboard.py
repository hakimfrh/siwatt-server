from pydantic import BaseModel

class DashboardStats(BaseModel):
    avg_usage_today: float
    token_balance: float
    estimated_days: int
    estimated_days_display: str
