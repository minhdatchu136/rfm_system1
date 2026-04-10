"""
flows/schedule.py — Đăng ký deployment và schedule với Prefect
Chạy: python flows/schedule.py deploy
"""

from prefect import serve
from prefect.client.schemas.schedules import CronSchedule
from flows.monthly_pipeline import monthly_pipeline, full_simulation
import sys
from datetime import datetime


def deploy():
    """Đăng ký cả hai flow lên Prefect server."""

    # Deploy 1: Monthly pipeline — chạy đầu mỗi tháng
    monthly_deployment = monthly_pipeline.to_deployment(
        name="rfm-monthly-scheduled",
        description="Tự động chạy đầu mỗi tháng — ETL → RFM → Recommendation",
        schedule=CronSchedule(cron="0 2 1 * *"),   # 2:00 AM ngày 1 hàng tháng
        parameters={
            "year" : datetime.now().year,
            "month": datetime.now().month,
        },
        tags=["rfm", "production", "monthly"],
    )

    # Deploy 2: Full simulation — chạy thủ công khi cần
    sim_deployment = full_simulation.to_deployment(
        name="rfm-full-simulation",
        description="Chạy thủ công để khởi tạo 13 tháng lịch sử",
        tags=["rfm", "simulation", "manual"],
    )

    print("Deploying flows to Prefect server...")
    serve(monthly_deployment, sim_deployment)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "deploy":
        deploy()
    else:
        print("Usage: python flows/schedule.py deploy")
