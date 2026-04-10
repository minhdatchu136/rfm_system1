"""
tests/test_pipeline.py — Kiểm thử cơ bản pipeline
Chạy: pytest tests/ -v
"""

import pytest
import pandas as pd
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestExtract:
    def test_extract_csv(self, tmp_path):
        """Extract đọc file CSV và trả về DataFrame."""
        # Tạo file CSV mẫu
        csv = tmp_path / "test.csv"
        csv.write_text(
            "InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n"
            "536365,85123A,TEST ITEM,6,12/1/2010 8:26,2.55,17850,United Kingdom\n"
        )
        from etl.extract import extract_data, validate_schema
        df = extract_data(str(csv))
        assert len(df) == 1
        validate_schema(df)  # không raise

    def test_filter_by_month(self):
        """filter_by_month lọc đúng dữ liệu tích lũy."""
        from etl.extract import filter_by_month
        df = pd.DataFrame({
            "InvoiceDate": ["1/15/2011 10:00", "3/20/2011 10:00",
                            "6/5/2011 10:00", "11/1/2011 10:00"],
            "CustomerID": ["1","2","3","4"],
        })
        result = filter_by_month(df, 2011, 3)
        assert len(result) == 2   # chỉ Jan + Mar
        result6 = filter_by_month(df, 2011, 6)
        assert len(result6) == 3  # Jan + Mar + Jun


class TestTransform:
    def setup_method(self):
        self.raw = pd.DataFrame({
            "InvoiceNo"  : ["536365","C536366","536367","536368"],
            "StockCode"  : ["A","B","C","D"],
            "Description": ["X","Y","Z","W"],
            "Quantity"   : [6, 2, -1, 4],
            "InvoiceDate": ["12/1/2010 8:26","12/1/2010 9:00",
                            "12/2/2010 10:00","12/2/2010 11:00"],
            "UnitPrice"  : [2.55, 3.0, 1.5, 0.0],
            "CustomerID" : [17850, 17851, 17852, 17853],
            "Country"    : ["UK","UK","UK","UK"],
        })

    def test_clean_removes_cancelled(self):
        from etl.transform import clean_data
        cleaned = clean_data(self.raw)
        assert not any(cleaned["InvoiceNo"].astype(str).str.startswith("C"))

    def test_clean_removes_negative_qty(self):
        from etl.transform import clean_data
        cleaned = clean_data(self.raw)
        assert (cleaned["Quantity"] > 0).all()

    def test_clean_removes_zero_price(self):
        from etl.transform import clean_data
        cleaned = clean_data(self.raw)
        assert (cleaned["UnitPrice"] > 0).all()

    def test_clean_creates_total_amount(self):
        from etl.transform import clean_data
        cleaned = clean_data(self.raw)
        assert "total_amount" in cleaned.columns
        assert (cleaned["total_amount"] > 0).all()

    def test_dim_product_no_duplicates(self):
        from etl.transform import clean_data, build_dim_product
        cleaned = clean_data(self.raw)
        dim     = build_dim_product(cleaned)
        assert dim["stock_code"].nunique() == len(dim)


class TestRFM:
    def setup_method(self):
        import pandas as pd
        from datetime import datetime
        self.df = pd.DataFrame({
            "customer_id"  : ["C1","C1","C2","C3","C3","C3"],
            "invoice_no"   : ["I1","I2","I3","I4","I5","I6"],
            "invoice_date" : pd.to_datetime([
                "2011-10-01","2011-11-01",
                "2011-01-01",
                "2011-09-01","2011-10-01","2011-11-01",
            ]),
            "total_amount" : [100.0, 200.0, 50.0, 80.0, 90.0, 150.0],
        })

    def test_compute_rfm_columns(self):
        from analytics.rfm import compute_rfm
        rfm = compute_rfm(self.df, reference_date="2011-12-01")
        assert set(["customer_id","Recency","Frequency","Monetary"]).issubset(rfm.columns)

    def test_recency_order(self):
        """Khách mua gần đây nhất phải có Recency nhỏ nhất."""
        from analytics.rfm import compute_rfm
        rfm = compute_rfm(self.df, reference_date="2011-12-01")
        r   = rfm.set_index("customer_id")["Recency"]
        assert r["C1"] < r["C2"]   # C1 mua Nov, C2 mua Jan

    def test_frequency_count(self):
        from analytics.rfm import compute_rfm
        rfm = compute_rfm(self.df, reference_date="2011-12-01")
        r   = rfm.set_index("customer_id")["Frequency"]
        assert r["C3"] == 3   # C3 có 3 hóa đơn
        assert r["C2"] == 1

    def test_score_range(self):
        from analytics.rfm import compute_rfm, score_rfm
        rfm    = compute_rfm(self.df, reference_date="2011-12-01")
        scored = score_rfm(rfm)
        assert scored["R_Score"].between(1,5).all()
        assert scored["F_Score"].between(1,5).all()
        assert scored["M_Score"].between(1,5).all()

    def test_segment_assigned(self):
        from analytics.rfm import compute_rfm, score_rfm, assign_segment
        rfm      = compute_rfm(self.df, reference_date="2011-12-01")
        scored   = score_rfm(rfm)
        segmented = assign_segment(scored)
        assert "Segment" in segmented.columns
        assert segmented["Segment"].notna().all()
        assert not segmented["Segment"].eq("Unclassified").any()
