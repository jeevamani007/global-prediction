import math
import re
import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from sqlalchemy import text
from database import engine


class BankingDomainDetector:

    def __init__(self):
        self.domain = "Banking"

        try:
            # Use raw SQL query to read from existing table (no primary key needed)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT keyword, column_name FROM banking_keywords"))
                data = result.fetchall()
            
            self.keywords = [row[0].lower() if row[0] else "" for row in data]
            self.columns_db = [row[1].lower() if row[1] else "" for row in data]
            
            # Filter out empty strings
            self.keywords = [k for k in self.keywords if k]
            self.columns_db = [c for c in self.columns_db if c]
            
        except Exception as e:
            # Fallback to empty lists if database query fails
            print(f"Warning: Could not load keywords from database: {e}")
            self.keywords = []
            self.columns_db = []

        self.synonyms = {
            "acct": "account",
            "accno": "account",
            "cust": "customer",
            "amt": "amount",
            "bal": "balance",
            "txn": "transaction",
            "ifsc": "ifsc",
            "branch": "branch"
        }

    def normalize(self, text):
        return str(text).lower().replace(" ", "").replace("_", "")

    # 🔍 Column value intelligence
    def value_pattern_score(self, series, column_name):
        score = 0
        col = series.dropna()

        if col.empty:
            return 0

        name = column_name.lower()

        if any(k in name for k in ["amount", "amt", "balance", "bal"]):
            if pd.to_numeric(col, errors="coerce").notna().mean() > 0.8:
                score += 1

        if "account" in name:
            if col.astype(str).str.isnumeric().mean() > 0.7:
                score += 1

        if "ifsc" in name:
            if col.astype(str).str.len().mean() == 11:
                score += 1

        if "date" in name:
            if pd.to_datetime(col, errors="coerce").notna().mean() > 0.7:
                score += 1

        return score

    def _looks_account_like_name(self, column_name: str) -> bool:
        norm = self.normalize(column_name)
        candidates = ["account", "acct", "accno", "custaccount", "customeraccount", "accnumber"]
        return any(key in norm for key in candidates)

    def _looks_account_like_values(self, series: pd.Series) -> bool:
        col = series.dropna().astype(str)
        if col.empty:
            return False
        digit_ratio = col.str.fullmatch(r"\d+").mean()
        length_ratio = col.str.len().between(6, 16).mean()
        return digit_ratio > 0.6 and length_ratio > 0.6

    def validate_account_numbers(self, df: pd.DataFrame):
        results = []
        total_rows = len(df)

        for col in df.columns:
            series = df[col]

            if not self._looks_account_like_name(col) and not self._looks_account_like_values(series):
                continue

            non_null = series.dropna().astype(str)
            if non_null.empty:
                continue

            digit_only_ratio = float(non_null.str.fullmatch(r"\d+").mean())
            length_ok_ratio = float(non_null.str.len().between(6, 16).mean())
            no_symbol_ratio = float(non_null.str.contains(r"[^0-9]").apply(lambda x: not x).mean())
            unique_ratio = float(non_null.nunique() / len(non_null))
            not_null_ratio = float(len(non_null) / max(len(series), 1))
            randomized_flag = bool(unique_ratio > 0.85 and digit_only_ratio > 0.8)

            violations = []
            for val in non_null.head(50):
                val_str = str(val)
                if not re.fullmatch(r"\d{6,16}", val_str):
                    violations.append(val_str)
                if len(violations) >= 5:
                    break

            weighted_score = (
                0.35 * digit_only_ratio
                + 0.25 * length_ok_ratio
                + 0.15 * unique_ratio
                + 0.10 * not_null_ratio
                + 0.10 * no_symbol_ratio
                + (0.05 if randomized_flag else 0)
            )
            prob = float(1 / (1 + math.exp(-6 * (weighted_score - 0.6))))

            results.append({
                "column": col,
                "probability_account_number": float(round(prob * 100, 2)),
                "decision": "match" if prob >= 0.6 else "not_match",
                "rules": {
                    "digits_only_ratio": round(digit_only_ratio, 3),
                    "length_6_16_ratio": round(length_ok_ratio, 3),
                    "unique_ratio": round(unique_ratio, 3),
                    "not_null_ratio": round(not_null_ratio, 3),
                    "no_symbol_ratio": round(no_symbol_ratio, 3),
                    "randomized": randomized_flag
                },
                "sample_violations": violations
            })

        summary = {
            "total_rows": total_rows,
            "checked_columns": len(results),
            "best_match": None
        }

        if results:
            best = max(results, key=lambda r: r["probability_account_number"])
            summary["best_match"] = {
                "column": best["column"],
                "probability_account_number": best["probability_account_number"],
                "decision": best["decision"]
            }

        return {
            "account_like_columns": results,
            "summary": summary
        }

    def predict(self, csv_path):
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            return {"error": str(e)}

        total_cols = len(df.columns)
        if total_cols == 0:
            return {"domain": "Unknown", "confidence": 0}

        matched_score = 0

        # ✅ NEW STORAGE
        matched_keywords = []
        matched_columns = []
        match_map = []

        details = []

        for col in df.columns:
            norm_col = self.normalize(col)
            best_score = 0
            best_keyword = None
            match_type = "none"

            # 1️⃣ keyword + column fuzzy
            for ref in self.keywords + self.columns_db:
                s = fuzz.ratio(norm_col, self.normalize(ref))
                if s > best_score:
                    best_score = s
                    best_keyword = ref
                    match_type = "fuzzy"

            # 2️⃣ synonym fallback
            if best_score < 85:
                for syn, actual in self.synonyms.items():
                    if syn in norm_col:
                        s = fuzz.ratio(actual, norm_col)
                        if s > best_score:
                            best_score = s
                            best_keyword = actual
                            match_type = "synonym"

            # 3️⃣ Name score
            if best_score >= 90:
                matched_score += 1
            elif best_score >= 75:
                matched_score += 0.5

            # 4️⃣ Value intelligence
            value_score = self.value_pattern_score(df[col], col)
            matched_score += value_score

            # ✅ STORE MATCHED KEYWORDS
            if best_score >= 75:
                matched_keywords.append(best_keyword)
                matched_columns.append(col)

                match_map.append({
                    "user_column": col,
                    "matched_keyword": best_keyword,
                    "name_score": best_score,
                    "value_score": value_score,
                    "match_type": match_type
                })

            details.append({
                "column": col,
                "name_score": best_score,
                "value_score": value_score,
                "match_type": match_type
            })

        # 5️⃣ Empty column penalty
        empty_cols = df.columns[df.isna().all()].tolist()
        matched_score -= len(empty_cols) * 0.2
        matched_score = max(0, matched_score)

        # 6️⃣ Ratio logic
        max_possible = total_cols * 2
        confidence_100 = round((matched_score / max_possible) * 100, 2)
        confidence_10 = round(confidence_100 / 10, 2)

        # 7️⃣ Final decision
        if confidence_100 >= 85:
            decision = "CONFIRMED_BANKING"
            qualitative = "Very Strong"
        elif confidence_100 >= 65:
            decision = "LIKELY_BANKING"
            qualitative = "Strong"
        else:
            decision = "UNKNOWN"
            qualitative = "Weak"

        # 8️⃣ Only do account-number prediction if banking is detected
        if decision != "UNKNOWN":
            account_number_validation = self.validate_account_numbers(df)
            best_match = account_number_validation.get("summary", {}).get("best_match") if account_number_validation else None
            account_number_check = {
                "has_account_number_column": bool(account_number_validation and account_number_validation.get("account_like_columns")),
                "best_match_column": best_match.get("column") if best_match else None,
                "best_match_decision": best_match.get("decision") if best_match else None,
                "best_match_probability": best_match.get("probability_account_number") if best_match else None
            }
        else:
            account_number_validation = None
            account_number_check = {
                "has_account_number_column": False,
                "best_match_column": None,
                "best_match_decision": None,
                "best_match_probability": None
            }

        return {
            "domain": self.domain if decision != "UNKNOWN" else "Unknown",
            "confidence_percentage": confidence_100,
            "confidence_out_of_10": confidence_10,
            "decision": decision,
            "qualitative": qualitative,
            "total_columns": total_cols,
            "empty_columns": empty_cols,

            # ✅ IMPORTANT OUTPUTS
            "matched_keywords": list(set(matched_keywords)),
            "matched_columns": matched_columns,
            "keyword_column_mapping": match_map,

            "details": details,
            "account_number_validation": account_number_validation,
            "account_number_check": account_number_check
        }
